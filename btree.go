// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package btree implements in-memory B-Trees of arbitrary degree.
//
// btree implements an in-memory B-Tree for use as an ordered data structure.
// It is not meant for persistent storage solutions.
//
// It has a flatter structure than an equivalent red-black or other binary tree,
// which in some cases yields better memory usage and/or performance.
// See some discussion on the matter here:
//   http://google-opensource.blogspot.com/2013/01/c-containers-that-save-memory-and-time.html
// Note, though, that this project is in no way related to the C++ B-Tree
// implementation written about there.
//
// Within this tree, each node contains a slice of items and a (possibly nil)
// slice of children.  For basic numeric values or raw structs, this can cause
// efficiency differences when compared to equivalent C++ template code that
// stores values in arrays within the node:
//   * Due to the overhead of storing values as interfaces (each
//     value needs to be stored as the value itself, then 2 words for the
//     interface pointing to that value and its type), resulting in higher
//     memory use.
//   * Since interfaces can point to values anywhere in memory, values are
//     most likely not stored in contiguous blocks, resulting in a higher
//     number of cache misses.
// These issues don't tend to matter, though, when working with strings or other
// heap-allocated structures, since C++-equivalent structures also must store
// pointers and also distribute their values across the heap.
//
// This implementation is designed to be a drop-in replacement to gollrb.LLRB
// trees, (http://github.com/petar/gollrb), an excellent and probably the most
// widely used ordered tree implementation in the Go ecosystem currently.
// Its functions, therefore, exactly mirror those of
// llrb.LLRB where possible.  Unlike gollrb, though, we currently don't
// support storing multiple equivalent values.
package btree

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

const (
	DefaultFreeListSize = 32
)

var (
	nilChildren = make(children, 16)
)

// FreeList represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTrees can share the same
// FreeList.
// Two Btrees using the same freelist are safe for concurrent write access.
type FreeList struct {
	mu       sync.Mutex
	freelist []*node
}

// NewFreeList creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

func (f *FreeList) newNode() (n *node) {
	f.mu.Lock()
	index := len(f.freelist) - 1
	if index < 0 {
		f.mu.Unlock()
		return new(node)
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	f.mu.Unlock()
	return
}

// freeNode adds the given node to the list, returning true if it was added
// and false if it was discarded.
func (f *FreeList) freeNode(n *node) (out bool) {
	f.mu.Lock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	f.mu.Unlock()
	return
}

// ItemIterator allows callers of Ascend* to iterate in-order over portions of
// the tree.  When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIterator func(i Item) bool

// New creates a new B-Tree with the given degree.
//
// New(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
func New(degree int) *BTree {
	return NewWithFreeList(degree, NewFreeList(DefaultFreeListSize))
}

// NewWithFreeList creates a new B-Tree that uses the given node free list.
func NewWithFreeList(degree int, f *FreeList) *BTree {
	if degree <= 1 || degree*2 > bitmaskSize { // at most degree*2-1 items per node
		panic("bad degree")
	}
	return &BTree{
		degree: degree,
		cow:    &copyOnWriteContext{freelist: f},
	}
}

// children stores child nodes in a node.
type children []*node

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *node {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index children. index must be less than or equal to length.
func (s *children) truncate(index int) {
	var toClear children
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   * len(children) == 0, len(items) unconstrained
//   * len(children) == len(items) + 1
//
// itemHasSuccessor (a bitmask) tells if the corresponding item's
// successor is present in the tree
//
// childIsDense (a bitmask) tells if 'itemHasSuccessor' holds for every
// item in the corresponding child subtree.
//
// Together, itemHasSuccessor and childIsDense are used to implement
// O(log(n)) spare ID allocation, which is defined as follows:
//
//   Generate the minimal key greater or equal to K that is NOT
//   in the tree.
//
type node struct {
	cow              *copyOnWriteContext
	children         children
	itemHasSuccessor bitmask
	childIsDense     bitmask
	items            items
}

func (n *node) mutableFor(cow *copyOnWriteContext) *node {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	out.items.assign(&n.items)
	// Copy children
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children, len(n.children), cap(n.children))
	}
	copy(out.children, n.children)
	out.itemHasSuccessor = n.itemHasSuccessor
	out.childIsDense = n.childIsDense
	return out
}

func (n *node) mutableChild(i int) *node {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

func (n *node) isDense() bit {
	if n.itemHasSuccessor.firstClearBitIndex(0) < n.items.len() {
		return 0
	}
	if n.childIsDense.firstClearBitIndex(0) < len(n.children) {
		return 0
	}
	return 1
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *node) split(i int) (Item, bit, *node) {
	item := n.items.get(i)
	next := n.cow.newNode()
	next.items.append(&n.items, i+1)
	next.itemHasSuccessor = n.itemHasSuccessor.bitsAt(i + 1)
	n.items.truncate(i)
	if len(n.children) > 0 {
		next.children = append(next.children, n.children[i+1:]...)
		next.childIsDense = n.childIsDense.bitsAt(i + 1)
		n.children.truncate(i + 1)
	}
	return item, n.itemHasSuccessor.bitAt(i), next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i, maxItems int) bool {
	if n.children[i].items.len() < maxItems {
		return false
	}
	first := n.mutableChild(i)
	item, itemHasSuccessor, second := first.split(maxItems / 2)
	n.items.insertAt(i, item)
	n.itemHasSuccessor.insertBitAt(i, itemHasSuccessor)
	n.children.insertAt(i+1, second)
	n.childIsDense.setBitAt(i, first.isDense())
	n.childIsDense.insertBitAt(i+1, second.isDense())
	return true
}

// insert inserts an item into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxItems items.  Should an equivalent item be
// be found/replaced by insert, it will be returned.
func (n *node) insert(item Item, maxItems int, hasSuccessor bit) Item {
	i, found := n.items.find(item)
	if found {
		out := n.items.get(i)
		n.items.set(i, item)
		return out
	}
	if i > 0 && n.items.predecessor(i-1, item) {
		n.itemHasSuccessor.setBitAt(i-1, 1)
	}
	if i < n.items.len() && n.items.successor(i, item) {
		hasSuccessor = 1
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		n.itemHasSuccessor.insertBitAt(i, hasSuccessor)
		return nil
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.items.get(i)
		switch {
		case less(item, inTree):
			if n.items.successor(i, item) {
				hasSuccessor = 1
			}
			// no change, we want first split node
		case less(inTree, item):
			if n.items.predecessor(i, item) {
				n.itemHasSuccessor.setBitAt(i, 1)
			}
			i++ // we want second split node
		default:
			out := n.items.get(i)
			n.items.set(i, item)
			return out
		}
	}
	out := n.mutableChild(i).insert(item, maxItems, hasSuccessor)
	n.childIsDense.setBitAt(i, n.children[i].isDense())
	return out
}

// get finds the given key in the subtree and returns it.
func (n *node) get(key Item) Item {
	i, found := n.items.find(key)
	if found {
		return n.items.get(i)
	} else if len(n.children) > 0 {
		return n.children[i].get(key)
	}
	return nil
}

// firstWithoutSuccessor finds the minimal item >= key that doesn't
// have a successor; returns nil if the key was not found
func (n *node) firstWithoutSuccessor(key Item) (bool, Item) {
	i, found := n.items.find(key)
	switch {
	case found:
		if n.itemHasSuccessor.bitAt(i) == 0 {
			return true, n.items.get(i)
		}
		i++
	case len(n.children) == 0:
		return true, nil
	case n.childIsDense.bitAt(i) == 0:
		ok, out := n.children[i].firstWithoutSuccessor(key)
		if ok {
			return true, out
		}
		i++
	case less(key, min(n.children[i])):
		return true, nil
	}
	out := n.firstWithoutSuccessorAt(i)
	return out != nil, out
}

// firstWithoutSuccessorAt finds the minimal item that doesn't have a
// successor, considering only indices >= i in the current node.
func (n *node) firstWithoutSuccessorAt(i int) Item {
	firstItemWithoutSuccessor := n.itemHasSuccessor.firstClearBitIndex(i)
	firstSparseChild := n.childIsDense.firstClearBitIndex(i)
	if firstSparseChild <= firstItemWithoutSuccessor && firstSparseChild < len(n.children) {
		return n.children[firstSparseChild].firstWithoutSuccessorAt(0)
	}
	if firstItemWithoutSuccessor < n.items.len() {
		return n.items.get(firstItemWithoutSuccessor)
	}
	return nil
}

// min returns the first item in the subtree.
func min(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[0]
	}
	if n.items.len() == 0 {
		return nil
	}
	return n.items.get(0)
}

// max returns the last item in the subtree.
func max(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[len(n.children)-1]
	}
	if n.items.len() == 0 {
		return nil
	}
	return n.items.get(n.items.len() - 1)
}

// toRemove details what item to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given item
	removeMin                  // removes smallest item in the subtree
	removeMax                  // removes largest item in the subtree

	detachMax = 0x10 | removeMax // like removeMax, the item to be put back into tree
)

// remove removes an item from the subtree rooted at this node.
func (n *node) remove(item Item, minItems int, typ toRemove) Item {
	var i int
	var found bool
	switch 0xf & typ { // coerce detachMax -> removeMax
	case removeMax:
		if len(n.children) == 0 {
			if typ != detachMax && n.items.len() > 1 {
				n.itemHasSuccessor.setBitAt(n.items.len()-2, 0)
			}
			return n.items.pop()
		}
		i = n.items.len()
	case removeMin:
		if len(n.children) == 0 {
			n.itemHasSuccessor.removeBitAt(0)
			return n.items.removeAt(0)
		}
		i = 0
	case removeItem:
		i, found = n.items.find(item)
		if len(n.children) == 0 {
			if found {
				if i > 0 {
					n.itemHasSuccessor.setBitAt(i-1, 0)
				}
				n.itemHasSuccessor.removeBitAt(i)
				return n.items.removeAt(i)
			}
			return nil
		}
	default:
		panic("invalid type")
	}
	// If we get to here, we have children.
	if n.children[i].items.len() <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ)
	}
	child := n.mutableChild(i)
	// Either we had enough items to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	if found {
		// The item exists at index 'i', and the child we've selected can give us a
		// predecessor, since if we've gotten here it's got > minItems items in it.
		out := n.items.get(i)
		// We use our special-case 'remove' call with typ=detachMax to pull the
		// predecessor of item i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the item from.
		n.items.set(i, child.remove(nil, minItems, detachMax))
		n.itemHasSuccessor.setBitAt(i, 0)
		n.childIsDense.setBitAt(i, child.isDense())
		return out
	}
	// Final recursive call.  Once we're here, we know that the item isn't in this
	// node and that the child is big enough to remove from.
	if out := child.remove(item, minItems, typ); out != nil {
		if i != 0 && typ != detachMax && n.items.predecessor(i-1, out) {
			n.itemHasSuccessor.setBitAt(i-1, 0)
		}
		n.childIsDense.setBitAt(i, child.isDense())
		return out
	}
	return nil
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// item from it while keeping it at minItems, then calls remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casing:
//   1) item is in this node
//   2) item is in child
// In both cases, we need to handle the two subcases:
//   A) node has enough values that it can spare one
//   B) node doesn't have enough values
// For the latter, we have to check:
//   a) left sibling has node to spare
//   b) right sibling has node to spare
//   c) we must merge
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough items, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough items and can guarantee
// that we hit case A.
func (n *node) growChildAndRemove(i int, item Item, minItems int, typ toRemove) Item {
	if i > 0 && n.children[i-1].items.len() > minItems {
		// Steal from left child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i - 1)

		child.itemHasSuccessor.insertBitAt(
			0, n.itemHasSuccessor.bitAt(i-1),
		)
		n.itemHasSuccessor.setBitAt(
			i-1, stealFrom.itemHasSuccessor.bitAt(stealFrom.items.len()-1),
		)

		stolenItem := stealFrom.items.pop()
		child.items.insertAt(0, n.items.get(i-1))
		n.items.set(i-1, stolenItem)

		if len(stealFrom.children) > 0 {
			child.childIsDense.insertBitAt(
				0, stealFrom.childIsDense.bitAt(len(stealFrom.children)-1),
			)
			child.children.insertAt(0, stealFrom.children.pop())
		}

		n.childIsDense.setBitAt(i, child.isDense())
		n.childIsDense.setBitAt(i-1, stealFrom.isDense())
	} else if i < n.items.len() && n.children[i+1].items.len() > minItems {
		// steal from right child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i + 1)

		child.itemHasSuccessor.setBitAt(
			child.items.len(), n.itemHasSuccessor.bitAt(i),
		)
		n.itemHasSuccessor.setBitAt(
			i, stealFrom.itemHasSuccessor.removeBitAt(0),
		)

		stolenItem := stealFrom.items.removeAt(0)
		child.items.push(n.items.get(i))
		n.items.set(i, stolenItem)

		if len(stealFrom.children) > 0 {
			child.childIsDense.setBitAt(
				len(child.children),
				stealFrom.childIsDense.removeBitAt(0),
			)
			child.children = append(child.children, stealFrom.children.removeAt(0))
		}

		n.childIsDense.setBitAt(i, child.isDense())
		n.childIsDense.setBitAt(i+1, stealFrom.isDense())
	} else {
		if i >= n.items.len() {
			i--
		}
		child := n.mutableChild(i)
		// merge with right child
		mergeItem := n.items.removeAt(i)
		mergeChild := n.children.removeAt(i + 1)

		child.itemHasSuccessor.setBitAt(
			child.items.len(),
			n.itemHasSuccessor.removeBitAt(i),
		)
		child.itemHasSuccessor.setBitsAt(
			child.items.len()+1,
			mergeChild.itemHasSuccessor,
		)
		child.childIsDense.setBitsAt(
			len(child.children),
			mergeChild.childIsDense,
		)
		n.childIsDense.setBitAt(i, child.isDense())
		n.childIsDense.removeBitAt(i + 1)

		child.items.push(mergeItem)
		child.items.append(&mergeChild.items, 0)
		child.children = append(child.children, mergeChild.children...)
		n.cow.freeNode(mergeChild)
	}
	return n.remove(item, minItems, typ)
}

type direction int

const (
	descend = direction(-1)
	ascend  = direction(+1)
)

// iterate provides a simple method for iterating over elements in the tree.
//
// When ascending, the 'start' should be less than 'stop' and when descending,
// the 'start' should be greater than 'stop'. Setting 'includeStart' to true
// will force the iterator to include the first item when it equals 'start',
// thus creating a "greaterOrEqual" or "lessThanEqual" rather than just a
// "greaterThan" or "lessThan" queries.
func (n *node) iterate(dir direction, start, stop Item, includeStart bool, hit bool, iter ItemIterator) (bool, bool) {
	var ok, found bool
	var index int
	switch dir {
	case ascend:
		if start != nil {
			index, _ = n.items.find(start)
		}
		for i := index; i < n.items.len(); i++ {
			if len(n.children) > 0 {
				if hit, ok = n.children[i].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if !includeStart && !hit && start != nil && !less(start, n.items.get(i)) {
				hit = true
				continue
			}
			hit = true
			if stop != nil && !less(n.items.get(i), stop) {
				return hit, false
			}
			if !iter(n.items.get(i)) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[len(n.children)-1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	case descend:
		if start != nil {
			index, found = n.items.find(start)
			if !found {
				index = index - 1
			}
		} else {
			index = n.items.len() - 1
		}
		for i := index; i >= 0; i-- {
			if start != nil && !less(n.items.get(i), start) {
				if !includeStart || hit || less(start, n.items.get(i)) {
					continue
				}
			}
			if len(n.children) > 0 {
				if hit, ok = n.children[i+1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if stop != nil && !less(stop, n.items.get(i)) {
				return hit, false //	continue
			}
			hit = true
			if !iter(n.items.get(i)) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.children[0].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	}
	return hit, true
}

// Used for testing/debugging purposes.
func (n *node) print(w io.Writer, level int) {
	fmt.Fprintf(w, "%sNODE:%v\n", strings.Repeat("  ", level), n.items)
	for _, c := range n.children {
		c.print(w, level+1)
	}
}

// BTree is an implementation of a B-Tree.
//
// BTree stores Item instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	degree int
	length int
	root   *node
	cow    *copyOnWriteContext
}

// copyOnWriteContext pointers determine node ownership... a tree with a write
// context equivalent to a node's write context is allowed to modify that node.
// A tree whose write context does not match a node's is not allowed to modify
// it, and must create a new, writable copy (IE: it's a Clone).
//
// When doing any write operation, we maintain the invariant that the current
// node's context is equal to the context of the tree that requested the write.
// We do this by, before we descend into any node, creating a copy with the
// correct context if the contexts don't match.
//
// Since the node we're currently visiting on any write has the requesting
// tree's context, that node is modifiable in place.  Children of that node may
// not share context, but before we descend into them, we'll make a mutable
// copy.
type copyOnWriteContext struct {
	freelist *FreeList
}

// Clone clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degredation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *BTree) Clone() (t2 *BTree) {
	// Create two entirely new copy-on-write contexts.
	// This operation effectively creates three trees:
	//   the original, shared nodes (old b.cow)
	//   the new b.cow nodes
	//   the new out.cow nodes
	cow1, cow2 := *t.cow, *t.cow
	out := *t
	t.cow = &cow1
	out.cow = &cow2
	return &out
}

// maxItems returns the max number of items to allow per node.
func (t *BTree) maxItems() int {
	return t.degree*2 - 1
}

// minItems returns the min number of items to allow per node (ignored for the
// root node).
func (t *BTree) minItems() int {
	return t.degree - 1
}

func (c *copyOnWriteContext) newNode() (n *node) {
	n = c.freelist.newNode()
	n.cow = c
	return
}

type freeType int

const (
	ftFreelistFull freeType = iota // node was freed (available for GC, not stored in freelist)
	ftStored                       // node was stored in the freelist for later use
	ftNotOwned                     // node was ignored by COW, since it's owned by another one
)

// freeNode frees a node within a given COW context, if it's owned by that
// context.  It returns what happened to the node (see freeType const
// documentation).
func (c *copyOnWriteContext) freeNode(n *node) freeType {
	if n.cow == c {
		// clear to allow GC
		n.items.truncate(0)
		n.children.truncate(0)
		n.cow = nil
		if c.freelist.freeNode(n) {
			return ftStored
		} else {
			return ftFreelistFull
		}
	} else {
		return ftNotOwned
	}
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned.
// Otherwise, nil is returned.
//
// nil cannot be added to the tree (will panic).
func (t *BTree) ReplaceOrInsert(item Item) Item {
	if item == nil {
		panic("nil item being added to BTree")
	}
	if t.root == nil {
		t.root = t.cow.newNode()
		t.root.items.push(item)
		t.length++
		return nil
	} else {
		t.root = t.root.mutableFor(t.cow)
		if t.root.items.len() >= t.maxItems() {
			item2, itemHasSuccessor, second := t.root.split(t.maxItems() / 2)
			oldroot := t.root
			t.root = t.cow.newNode()
			t.root.items.push(item2)
			t.root.itemHasSuccessor = bitmask(itemHasSuccessor)
			t.root.children = append(t.root.children, oldroot, second)
			// rightmost subtree is sparse by definition
			t.root.childIsDense = bitmask(oldroot.isDense())
		}
	}
	out := t.root.insert(item, t.maxItems(), 0) // haven't seen item's successor yet, hence 0
	if out == nil {
		t.length++
	}
	return out
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns nil.
func (t *BTree) Delete(item Item) Item {
	return t.deleteItem(item, removeItem)
}

// DeleteMin removes the smallest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMin() Item {
	return t.deleteItem(nil, removeMin)
}

// DeleteMax removes the largest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMax() Item {
	return t.deleteItem(nil, removeMax)
}

func (t *BTree) deleteItem(item Item, typ toRemove) Item {
	if t.root == nil || t.root.items.len() == 0 {
		return nil
	}
	t.root = t.root.mutableFor(t.cow)
	out := t.root.remove(item, t.minItems(), typ)
	if t.root.items.len() == 0 && len(t.root.children) > 0 {
		oldroot := t.root
		t.root = t.root.children[0]
		t.cow.freeNode(oldroot)
	}
	if out != nil {
		t.length--
	}
	return out
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *BTree) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, greaterOrEqual, lessThan, true, false, iterator)
}

// AscendLessThan calls the iterator for every value in the tree within the range
// [first, pivot), until iterator returns false.
func (t *BTree) AscendLessThan(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, pivot, false, false, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (t *BTree) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, pivot, nil, true, false, iterator)
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (t *BTree) Ascend(iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, nil, false, false, iterator)
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (t *BTree) DescendRange(lessOrEqual, greaterThan Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, lessOrEqual, greaterThan, true, false, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the tree within the range
// [pivot, first], until iterator returns false.
func (t *BTree) DescendLessOrEqual(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, pivot, nil, true, false, iterator)
}

// DescendGreaterThan calls the iterator for every value in the tree within
// the range [last, pivot), until iterator returns false.
func (t *BTree) DescendGreaterThan(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, pivot, false, false, iterator)
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (t *BTree) Descend(iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, nil, false, false, iterator)
}

// Get looks for the key item in the tree, returning it.  It returns nil if
// unable to find that item.
func (t *BTree) Get(key Item) Item {
	if t.root == nil {
		return nil
	}
	return t.root.get(key)
}

// FirstWithoutSuccessor returns the first item in the key sort order,
// without successor, greater or equal to 'key'.  It returns nil if key
// was not found.
//
// Useful for fast spare ID allocation:
//
// if k := t.FirstWithoutSuccessor(nextID); k != nil {
//     id = k + 1
// } else {
//     id = nextID
// }
//
func (t *BTree) FirstWithoutSuccessor(key Item) Item {
	if t.root == nil {
		return nil
	}
	_, out := t.root.firstWithoutSuccessor(key)
	return out
}

// Min returns the smallest item in the tree, or nil if the tree is empty.
func (t *BTree) Min() Item {
	return min(t.root)
}

// Max returns the largest item in the tree, or nil if the tree is empty.
func (t *BTree) Max() Item {
	return max(t.root)
}

// Has returns true if the given key is in the tree.
func (t *BTree) Has(key Item) bool {
	return t.Get(key) != nil
}

// Len returns the number of items currently in the tree.
func (t *BTree) Len() int {
	return t.length
}

// Clear removes all items from the btree.  If addNodesToFreelist is true,
// t's nodes are added to its freelist as part of this call, until the freelist
// is full.  Otherwise, the root node is simply dereferenced and the subtree
// left to Go's normal GC processes.
//
// This can be much faster
// than calling Delete on all elements, because that requires finding/removing
// each element in the tree and updating the tree accordingly.  It also is
// somewhat faster than creating a new tree to replace the old one, because
// nodes from the old tree are reclaimed into the freelist for use by the new
// one, instead of being lost to the garbage collector.
//
// This call takes:
//   O(1): when addNodesToFreelist is false, this is a single operation.
//   O(1): when the freelist is already full, it breaks out immediately
//   O(freelist size):  when the freelist is empty and the nodes are all owned
//       by this tree, nodes are added to the freelist until full.
//   O(tree size):  when all nodes are owned by another tree, all nodes are
//       iterated over looking for nodes to add to the freelist, and due to
//       ownership, none are.
func (t *BTree) Clear(addNodesToFreelist bool) {
	if t.root != nil && addNodesToFreelist {
		t.root.reset(t.cow)
	}
	t.root, t.length = nil, 0
}

// reset returns a subtree to the freelist.  It breaks out immediately if the
// freelist is full, since the only benefit of iterating is to fill that
// freelist up.  Returns true if parent reset call should continue.
func (n *node) reset(c *copyOnWriteContext) bool {
	for _, child := range n.children {
		if !child.reset(c) {
			return false
		}
	}
	return c.freeNode(n) != ftFreelistFull
}
