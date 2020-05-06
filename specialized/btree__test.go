// btree__test.go
//
// Defines an item type used in tests. Also defines routines to create
// and manipulate items.
//
// This is intended as a customisation site for tests - should be
// possible to make tests work with customised BTree-s by altering just
// this file. See notes in btree_item.go concerning customisation.

package btree

// makeItem produces an item with the given key
func makeItem(i int) Item {
	return &entity{key: i}
}

// getInt extracts the key from an item
func getInt(i Item) int {
	return i.key
}
