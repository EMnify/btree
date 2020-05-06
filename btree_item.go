// btree_item.go
//
// Defines an Item, ways to compare items, and how items are stored in a
// BTree node.
//
// This is intended as a customization site - the intended usage is to
// copy the main btree.go and to put a custom btree_item.go alongside.
//
// There are two primary reasons for customisation.
//
// 1) Stock BTree defines Item as an interface implementing Less method.
//    One may replace that with a concrete type to make BTree faster and
//    leaner.
//
// 2) For performance reasons it is beneficial to cache integer keys in
//    a BTree node (fewer cache misses).

package btree

var (
	nilItems = make([]Item, 16)
)

// Item represents a single object in the tree.
type Item interface {
	// Less tests whether the current item is less than the given argument.
	//
	// This must provide a strict weak ordering.
	// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
	// hold one of either a or b in the tree).
	Less(than Item) bool

	// Predecessor tests whether the current item is the max value less
	// than the given argument.
	Predecessor(of Item) bool
}

// less tests whether the first argument is less than the second one;
// used by BTree to compare items.
func less(a Item, b Item) bool {
	return a.Less(b)
}

// items stores items in a node.
type items []Item

// assign overwrites existing items with new items;
// used by BTree when cloning a node
func (s *items) assign(i *items) {
	if cap(*s) >= len(*i) {
		*s = (*s)[:len(*i)]
	} else {
		*s = make([]Item, len(*i), cap(*i))
	}
	copy(*s, *i)
}

// get returns i-th item.
func (s items) get(i int) Item {
	return s[i]
}

// set updates i-th item.
func (s items) set(i int, in Item) {
	s[i] = in
}

// len returns the number of items in the list.
func (s items) len() int {
	return len(s)
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return item
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out Item) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// push appends an item to the list.
func (s *items) push(in Item) {
	*s = append(*s, in)
}

// append takes a slice of the second list starting at offset and
// appends it.
func (s *items) append(i *items, offset int) {
	*s = append(*s, (*i)[offset:]...)
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items) truncate(index int) {
	var toClear []Item
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItems):]
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s items) find(item Item) (index int, found bool) {
	i, j := 0, len(s)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if item.Less(s[h]) {
			j = h
		} else {
			i = h + 1
		}
	}
	if i > 0 && !s[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

// predecessor tells whether the i-th element is the predecessor of the
// passed item.
func (s items) predecessor(index int, item Item) bool {
	return s[index].Predecessor(item)
}

// successor tells whether the i-th element is the successor of the
// passed item.
func (s items) successor(index int, item Item) bool {
	return item.Predecessor(s[index])
}
