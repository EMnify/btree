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

type entity struct {
	key int
}

type Item *entity

// less tests whether the first argument is less than the second one;
// used by BTree to compare items.
func less(a Item, b Item) bool {
	return a.key < b.key
}

// items stores items in a node.
type items struct {
	length int
	keys   [bitmaskSize]int
	items  [bitmaskSize]Item
}

// assign overwrites existing items with new items;
// used by BTree when cloning a node
func (s *items) assign(i *items) {
	s.length = i.length
	copy(s.items[:], i.items[:i.length])
	copy(s.keys[:], i.keys[:i.length])
}

// get returns i-th item.
func (s *items) get(i int) Item {
	return s.items[i]
}

// set updates i-th item.
func (s *items) set(i int, in Item) {
	s.items[i] = in
	s.keys[i] = in.key
}

// len returns the number of items in the list.
func (s *items) len() int {
	return s.length
}

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, item Item) {
	copy(s.items[index+1:s.length+1], s.items[index:s.length])
	copy(s.keys[index+1:s.length+1], s.keys[index:s.length])
	s.items[index] = item
	s.keys[index] = item.key
	s.length++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) Item {
	item := s.items[index]
	copy(s.items[index:s.length-1], s.items[index+1:s.length])
	copy(s.keys[index:s.length-1], s.keys[index+1:s.length])
	s.length--
	s.items[s.length] = nil
	return item
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out Item) {
	s.length--
	out = s.items[s.length]
	s.items[s.length] = nil
	return
}

// push appends an item to the list.
func (s *items) push(in Item) {
	s.items[s.length] = in
	s.keys[s.length] = in.key
	s.length++
}

// append takes a slice of the second list starting at offset and
// appends it.
func (s *items) append(i *items, offset int) {
	copy(s.items[s.length:], i.items[offset:i.length])
	copy(s.keys[s.length:], i.keys[offset:i.length])
	s.length += i.length - offset
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items) truncate(index int) {
	for s.length > index {
		s.length--
		s.items[s.length] = nil
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s *items) find(item Item) (index int, found bool) {
	i, j := 0, s.length
	for i != j {
		if item.key <= s.keys[i] {
			if item.key == s.keys[i] {
				return i, true
			} else {
				return i, false
			}
		}
		i++
	}
	return i, false
}

// predecessor tells whether the i-th element is the predecessor of the
// passed item.
func (s *items) predecessor(index int, item Item) bool {
	return s.keys[index]+1 == item.key
}

// successor tells whether the i-th element is the successor of the
// passed item.
func (s *items) successor(index int, item Item) bool {
	return item.key+1 == s.keys[index]
}
