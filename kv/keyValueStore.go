package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"main/infrastructure"
)

var (
	Package = "kv"
	// ErrNotFound is returned when the key supplied to a Get or Delete method does not exist in the database or the
	// store does not exist at the given path.
	ErrNotFound = errors.New(Package + " - key not found")

	// ErrBadValue is returned when the value supplied to the Put method is invalid
	ErrBadValue = errors.New(Package + " - bad value")

	// ErrSameKeyTwice is returned when the same key is twice in the tree
	ErrSameKeyTwice = errors.New(Package + " - same key twice")

	// ErrOutOfRange is returned when the supplied size is too large
	ErrOutOfRange = errors.New(Package + " - size' is out of range")

	// ErrInvalidPath is returned when the path that has been given is not valid (inexistent/not writable)
	ErrInvalidPath = errors.New(Package + " - 'path' is not valid")
)

// Optimal max branching factor with our page structrue would be:  PageSize - Sizeof(bool) - 2x Sizeof(PageId)  / Sizeof(key,value)
// Note that Sizeof(PageId) describes the
const MAX_BRANCHING_FACTOR = 10 // int(((float32(infrastructure.PageSize - 1 - 16)) * 0.8) / 18)

type Page = infrastructure.Page

type KeyValueStore interface {
	Get(uint64) ([]byte, error) // Returns an error if the given key is not found
	Put(uint64, [10]byte) error // Returns an error on inserting same key twice
	Delete(uint64) error        // Returns an error on deleting same key twice
	ScanRange(uint64, uint64)   // Inclusive beginning key, exclusive end key

	// Control interface
	Create(string, int) (KeyValueStore, error) // Creates a KeyValueStore with a given path and size
	Open(string) (KeyValueStore, error)        // Opens a KeyValueStore stored at the given path
	Close() error                              // Closes the KeyValueStore
	DeleteStore(string) error                  // Deletes the KeyValueStore. Error if no KeyValueStore at that path
}

type bpTreeImpl struct {
	MaxMem     int
	Path       string
	root       *Node
	rootPageId int
}

// Public fields in this struct will be serialized by gob serializer
type Node struct {
	page     *Page
	PageId   int
	numKeys  int
	IsLeaf   bool
	Keys     [MAX_BRANCHING_FACTOR]uint64
	Values   [MAX_BRANCHING_FACTOR + 1][10]byte
	Children [MAX_BRANCHING_FACTOR + 1]Inode
	Parent   Inode
	Next     Inode
}

type Inode struct {
	PageId  int
	pointer *Node
}

func (k *bpTreeImpl) Create(path string, size int) (*bpTreeImpl, error) {

	k.MaxMem = 1 << (10 * 3) // 1 GB
	k.Path = "."             // create in local directory

	if size > k.MaxMem || size == 0 {
		return nil, ErrOutOfRange
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, ErrInvalidPath
	}

	// Initialize bufferPoolManager
	diskManager := infrastructure.NewDiskManagerMock()
	clockReplacer := infrastructure.NewClockReplacer(infrastructure.PoolSize)
	bufferPoolManager = *infrastructure.NewBufferPoolManager(diskManager, clockReplacer)

	// Create root node
	var bpTree bpTreeImpl
	rootPage := bufferPoolManager.NewPage()

	rootPage.IncPinCount()

	var root Node
	root.page = rootPage
	bpTree.root = &root
	bpTree.rootPageId = int(rootPage.GetId())

	bufferPoolManager.FlushPage(rootPage.GetId())

	return &bpTree, nil
}

var bufferPoolManager infrastructure.BufferPoolManager // Move to bpTree?

func getNodeFromPageId(pageId int) *Node {
	page := bufferPoolManager.FetchPage(infrastructure.PageID(pageId))
	return initializeNodeFromData(*page.GetData())
}

func (node *Node) writeNodeToPage() {
	data := node.serializeNode()
	page := bufferPoolManager.FetchPage(infrastructure.PageID(node.PageId))

	page.SetData(&data)
}

func initializeNodeFromData(data []byte) *Node {
	var node Node
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&node); err != nil {
		panic(node)
	}

	return &node
}

func (node *Node) serializeNode() []byte {

	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(node); err != nil {
		panic(err)
	}

	if buf.Len() > infrastructure.PageSize {
		fmt.Println(buf.Len())
		panic("Node too big for a single page size")
	}

	return buf.Bytes()
}

func (bpTree bpTreeImpl) Get(key uint64) ([10]byte, error) {
	var retValue [10]byte
	var root *Node = getNodeFromPageId(bpTree.rootPageId)

	if root == nil {
		return retValue, ErrNotFound
	}

	iteratorNode := root

	for !iteratorNode.IsLeaf {
		for i := 0; i < iteratorNode.numKeys; i++ {
			if key < iteratorNode.Keys[i] {
				bufferPoolManager.UnpinPage(iteratorNode.page.GetId(), false)
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i].PageId)
				break
			}
			if i == iteratorNode.numKeys-1 {
				bufferPoolManager.UnpinPage(iteratorNode.page.GetId(), false)
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i+1].PageId)
			}
		}
	}

	for i := 0; i < iteratorNode.numKeys; i++ {
		if iteratorNode.Keys[i] == key {
			return iteratorNode.Values[i], nil
		}
	}

	return retValue, ErrNotFound
}

func (bpTree *bpTreeImpl) Put(key uint64, value [10]byte) error {
	// Root node should always be pinned

	// Empty bpTree insert at root
	if bpTree.root.numKeys == 0 {
		node := bpTree.root
		node.Keys[0] = key
		node.Values[0] = value
		node.IsLeaf = true
		node.numKeys = 1

		bpTree.root.writeNodeToPage()

		return nil
	}

	var iteratorNode *Node = bpTree.root
	var parent *Node

	for iteratorNode.IsLeaf == false {
		parent = iteratorNode
		for i := 0; i < parent.numKeys; i++ {
			// Travers pointer to the left of tree (key < fence pointer)
			if key < iteratorNode.Keys[i] {
				bufferPoolManager.UnpinPage(iteratorNode.page.GetId(), false)
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i].PageId)
				break
			}

			// Travers pointer to the right of tree (key > fence pointer)
			if i == iteratorNode.numKeys-1 {
				bufferPoolManager.UnpinPage(iteratorNode.page.GetId(), false)
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i+1].PageId)
				break
			}
		}
	}

	// Current node has space
	if iteratorNode.numKeys < MAX_BRANCHING_FACTOR {

		// Find insertion point
		i := 0
		for key > iteratorNode.Keys[i] && i < iteratorNode.numKeys {
			i++
		}

		if key == iteratorNode.Keys[i] {
			return ErrSameKeyTwice
		}

		// Move keys
		for j := iteratorNode.numKeys; j > i; j-- {
			iteratorNode.Keys[j] = iteratorNode.Keys[j-1]
		}

		// Insert at found position
		iteratorNode.Keys[i] = key
		iteratorNode.numKeys++

		// Shift pointers
		iteratorNode.Children[iteratorNode.numKeys] = iteratorNode.Children[iteratorNode.numKeys-1]
		iteratorNode.Children[iteratorNode.numKeys-1] = Inode{} // or nil

		iteratorNode.writeNodeToPage()
		bufferPoolManager.UnpinPage(iteratorNode.page.GetId(), true)
	}
	return nil
}

/*
	 else // Current node has no space
	{
		var newLeaf Node
		var copyKeys [MAX_BRANCHING_FACTOR + 2]uint64

		// Copy keys from current node
		copy(copyKeys[:MAX_BRANCHING_FACTOR], iterator.keys[:MAX_BRANCHING_FACTOR])

		// Find insertion point
		i := 0
		for key > copyKeys[i] && i < MAX_BRANCHING_FACTOR {
			i++
		}

		if key == iterator.keys[i] {
			return ErrSameKeyTwice
		}

		for j := MAX_BRANCHING_FACTOR + 1; j > i; j-- {
			copyKeys[j] = copyKeys[j-1]
		}

		copyKeys[i] = key
		L := (MAX_BRANCHING_FACTOR + 1) / 2
		iterator.num_keys = L

		// Create new leaf
		newLeaf.ISLEAF = true
		newLeaf.values[0] = value
		newLeaf.num_keys = MAX_BRANCHING_FACTOR + 1 - L
		// Point current node to new leaf
		iterator.pointers[iterator.num_keys] = &newLeaf

		newLeaf.pointers[newLeaf.num_keys] = iterator.pointers[MAX_BRANCHING_FACTOR]
		iterator.pointers[MAX_BRANCHING_FACTOR] = nil

		copy(iterator.keys[:iterator.num_keys], copyKeys[:iterator.num_keys])

		k := iterator.num_keys
		for i := 0; i < newLeaf.num_keys; i++ {
			newLeaf.keys[i] = copyKeys[k]
			k++
		}

		if iterator == bpTree.root {
			var newRoot Node

			newRoot.keys[0] = newLeaf.keys[0]
			newRoot.pointers[0] = iterator
			newRoot.pointers[1] = &newLeaf
			newRoot.ISLEAF = false
			newRoot.num_keys = 1
			bpTree.root = &newRoot
		} else {
			internalInsertion(newLeaf.keys[0], parent, &newLeaf, bpTree.root)
		}
	}
	return nil
}
*/

/*
func internalInsertion(key uint64, iterator *Node, child *Node, root *Node) error {

	// Enough space to add a new key
	if iterator.num_keys < MAX_BRANCHING_FACTOR {
		i := 0
		for key > iterator.keys[i] && i < iterator.num_keys {
			i++
		}

		// Make space for new key and pointer
		for j := iterator.num_keys; j > i; j-- {
			iterator.keys[j] = iterator.keys[j-1]
		}
		for j := iterator.num_keys + 1; j > i; j-- {
			iterator.pointers[j] = iterator.pointers[j-1]
		}

		iterator.keys[i] = key
		iterator.num_keys++
		iterator.pointers[i+1] = child

	} else { // Node is full, need to split
		var newNode Node

		var copyKeys [MAX_BRANCHING_FACTOR + 1]uint64
		var copyPointers [MAX_BRANCHING_FACTOR + 2]*Node

		// TODO Refactor
		for i := 0; i < MAX_BRANCHING_FACTOR; i++ {
			copyKeys[i] = iterator.keys[i]
		}

		for i := 0; i < MAX_BRANCHING_FACTOR+1; i++ {
			copyPointers[i] = iterator.pointers[i]
		}

		i := 0
		for key > copyKeys[i] && i < MAX_BRANCHING_FACTOR {
			i++
		}

		for j := MAX_BRANCHING_FACTOR + 1; j > i; j-- {
			copyKeys[j] = copyKeys[j-1]
		}
		copyKeys[i] = key
		for j := MAX_BRANCHING_FACTOR + 2; j > i; j-- {
			copyPointers[j] = copyPointers[j-1]
		}

		copyPointers[i+1] = child
		newNode.ISLEAF = false
		var L = (MAX_BRANCHING_FACTOR + 1) / 2
		iterator.num_keys = L

		newNode.num_keys = MAX_BRANCHING_FACTOR - L

		var k = iterator.num_keys + 1
		for i := 0; i < newNode.num_keys; i++ {
			newNode.keys[i] = copyKeys[k]
			k++
		}

		k = iterator.num_keys + 1
		for i := 0; i < newNode.num_keys+1; i++ {
			newNode.pointers[i] = copyPointers[k]
			k++
		}

		if iterator == root {
			var newRoot Node
			newRoot.keys[0] = iterator.keys[iterator.num_keys]
			newRoot.pointers[0] = iterator
			newRoot.pointers[1] = &newNode
			newRoot.ISLEAF = false
			newRoot.num_keys = 1
			root = &newRoot
		} else {
			internalInsertion(iterator.keys[iterator.num_keys], findParent(root, iterator), &newNode, root)
		}
	}

	return nil
}
*/

/*
func findParent(iterator *Node, child *Node) *Node {
	var parent *Node

	if iterator.ISLEAF || iterator.pointers[0].ISLEAF {
		return nil
	}

	for i := 0; i < iterator.num_keys+1; i++ {
		if iterator.pointers[i] == child {
			parent = iterator
			return parent
		} else {
			parent = findParent(iterator.pointers[i], child)
			if parent != nil {
				return parent
			}
		}
	}
	return parent
}
*/

func (k *bpTreeImpl) Delete(key uint64) error {
	return nil
}

// ScanRange Returns all values with keys ranging [begin, end) (i.e. range with begin, but without end)
func (k *bpTreeImpl) ScanRange(begin uint64, end uint64) [][10]byte {
	return nil
}

func Open(path string) (*bpTreeImpl, error) {
	return nil, nil
}

func (k *bpTreeImpl) Close() error {
	return nil
}

func (k *bpTreeImpl) DeleteStore(path string) error {
	return nil
}
