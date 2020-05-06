// btree__test.go
//
// Defines an item type used in tests. Also defines routines to create
// and manipulate items.
//
// This is intended as a customisation site for tests - should be
// possible to make tests work with customised BTree-s by altering just
// this file. See notes in btree_item.go concerning customisation.

package btree

// Int implements the Item interface for integers.
type Int int

// Less returns true if int(a) < int(b).
func (a Int) Less(b Item) bool {
	return a < b.(Int)
}

// Predecessor returns true if int(a) < int(b) && int(a) + 1 == int(b)
func (a Int) Predecessor(b Item) bool {
	lhs, rhs := int(a), int(b.(Int))
	return lhs < rhs && lhs+1 == rhs
}

// makeItem produces an item with the given key
func makeItem(i int) Item {
	return Int(i)
}

// getInt extracts the key from an item
func getInt(i Item) int {
	return int(i.(Int))
}
