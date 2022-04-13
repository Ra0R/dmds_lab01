package kv

import (
	"encoding/gob"
	"errors"
	"main/infrastructure"
	"os"
	"unsafe"
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

	// Control interface
	Create(string, int) (KeyValueStore, error) // Creates a KeyValueStore with a given path and size
	Open(string) (KeyValueStore, error)        // Opens a KeyValueStore stored at the given path
	Close() error                              // Closes the KeyValueStore
	DeleteStore(string) error                  // Deletes the KeyValueStore. Error if no KeyValueStore at that path
}

type BpTreeImpl struct {
	MaxMem     int
	Path       string
	RootPageId int
}
type Node struct {
	IsLeaf       bool
	PageId       int
	ParentPageId int
	NextPageId   int
	numKeys      int
	Keys         [MAX_BRANCHING_FACTOR + 1]int
	Values       [MAX_BRANCHING_FACTOR + 1][10]byte
	Children     [MAX_BRANCHING_FACTOR + 1]int
	page         *Page
}

func (k *BpTreeImpl) Create(Path string, size int) (*BpTreeImpl, error) {

	if size <= 0 {
		k.MaxMem = 1 << (10 * 3) // 1 GB = Default value
	} else {
		k.MaxMem = size
	}

	if len(Path) > 0 {
		k.Path = Path
	} else {
		k.Path = "." // create in local directory
	}

	tree, _ := OpenKVStore(k.Path)
	if tree != nil {
		return nil, ErrInvalidPath
	}

	// Initialize bufferPoolManager
	diskManager := infrastructure.NewDiskManagerMock()
	clockReplacer := infrastructure.NewClockReplacer(infrastructure.PoolSize)
	bufferPoolManager = *infrastructure.NewBufferPoolManager(diskManager, clockReplacer)

	// Create root node
	var bpTree BpTreeImpl
	rootPage := bufferPoolManager.NewPage(k.Path)

	rootPage.IncPinCount()

	var root Node
	root.page = rootPage
	root.IsLeaf = true
	bpTree.Path = Path
	bpTree.RootPageId = int(rootPage.GetId())

	bufferPoolManager.FlushPage(rootPage.GetId())

	CreateKVStore(bpTree)

	return &bpTree, nil
}

var bufferPoolManager infrastructure.BufferPoolManager // Move to bpTree?

func getNodeFromPageId(pageId int) *Node {
	page := bufferPoolManager.FetchPage(infrastructure.PageID(pageId))
	return initializeNodeFromData(page.GetData())
}

func (node *Node) writeNodeToPage() {
	data := node.serializeNode()
	page := bufferPoolManager.FetchPage(infrastructure.PageID(node.PageId))

	if page != nil { // Unpersisted page
		page.SetData(data)
	} else {
		bufferPoolManager.NewPage(page.Path).SetData(data)
	}
}

func initializeNodeFromData(data []byte) *Node {
	var node Node
	if len(data) == 0 {
		return &node
	}

	curIndex := 0
	node.IsLeaf = *(*bool)(unsafe.Pointer(&data[0]))
	curIndex += 1

	var intInit int
	intSize := (int)(unsafe.Sizeof(intInit))
	node.PageId = *(*int)(unsafe.Pointer(&data[curIndex]))
	curIndex += intSize

	// PageId of parent
	node.ParentPageId = *(*int)(unsafe.Pointer(&data[curIndex]))
	curIndex += intSize

	// PageId of next
	node.NextPageId = *(*int)(unsafe.Pointer(&data[curIndex]))
	curIndex += intSize

	// Get numKeys
	node.numKeys = *(*int)(unsafe.Pointer(&data[curIndex]))
	curIndex += intSize

	if node.IsLeaf == false {
		// Parsing inner node
		for i := 0; i < node.numKeys+1; i++ {
			node.Children[i] = *(*int)(unsafe.Pointer(&data[curIndex]))
			curIndex += (int)(unsafe.Sizeof(node.Children[i]))
			node.Keys[i] = *(*int)(unsafe.Pointer(&data[curIndex]))
			curIndex += (int)(unsafe.Sizeof(node.Keys[i]))
		}

	} else {
		// Parsing leaf node
		for i := 0; i < node.numKeys+1; i++ {
			node.Keys[i] = *(*int)(unsafe.Pointer(&data[curIndex]))
			curIndex += (int)(unsafe.Sizeof(node.Keys[i]))
			node.Values[i] = *(*[10]byte)(unsafe.Pointer(&data[curIndex]))
			curIndex += 10
		}
	}

	return &node
}

func (node *Node) serializeNode() []byte {
	var data [infrastructure.PageSize]byte
	//isLeaf
	currentIndex := 0
	data[currentIndex] = *(*byte)(unsafe.Pointer(&node.IsLeaf))
	currentIndex++

	// PageId
	data[currentIndex] = *(*byte)(unsafe.Pointer(&node.PageId))
	var len = (int)(unsafe.Sizeof(node.PageId))
	currentIndex += len

	// ParentPageId
	data[currentIndex] = *(*byte)(unsafe.Pointer(&node.ParentPageId))
	currentIndex += len

	// NextPageId
	data[currentIndex] = *(*byte)(unsafe.Pointer(&node.NextPageId))
	currentIndex += len

	// numKeys
	data[currentIndex] = *(*byte)(unsafe.Pointer(&node.numKeys))
	currentIndex += len

	if node.IsLeaf {
		for i := 0; i < node.numKeys; i++ {
			data[currentIndex] = *(*byte)(unsafe.Pointer(&node.Keys[i]))
			var len = (int)(unsafe.Sizeof(node.Keys[i]))
			currentIndex += len
			for j := 0; j < 10; j++ {
				data[currentIndex] = *(*byte)(unsafe.Pointer(&node.Values[i][j]))
				currentIndex++
			}
		}
	} else {
		for i := 0; i < MAX_BRANCHING_FACTOR+1; i++ {
			data[currentIndex] = *(*byte)(unsafe.Pointer(&node.Children[i]))
			var len = (int)(unsafe.Sizeof(node.Children[i]))
			currentIndex += len
			data[currentIndex] = *(*byte)(unsafe.Pointer(&node.Keys[i]))
			currentIndex += len
		}
	}

	return data[:]
}

func (bpTree BpTreeImpl) Get(key int) ([10]byte, error) {
	var retValue [10]byte
	var root *Node = getNodeFromPageId(bpTree.RootPageId)

	if root == nil {
		return retValue, ErrNotFound
	}

	iteratorNode := root

	for !iteratorNode.IsLeaf {
		for i := 0; i < iteratorNode.numKeys; i++ {
			if key < iteratorNode.Keys[i] {
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i])
				break
			}
			if i == iteratorNode.numKeys-1 {
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i])
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

func (bpTree *BpTreeImpl) Put(key int, value [10]byte) error {
	// Root node should always be pinned
	rootNode := getNodeFromPageId(bpTree.RootPageId)

	// Empty bpTree insert at root
	if rootNode.numKeys == 0 {
		rootNode.PageId = bpTree.RootPageId
		rootNode.Keys[0] = key
		rootNode.Values[0] = value
		rootNode.IsLeaf = true
		rootNode.numKeys = 1

		rootNode.writeNodeToPage()

		return nil
	}

	var iteratorNode *Node = getNodeFromPageId(bpTree.RootPageId)
	parent := iteratorNode

	for iteratorNode.IsLeaf == false {
		parent = iteratorNode
		for i := 0; i < parent.numKeys; i++ {
			// Travers pointer to the left of tree (key < fence pointer)
			if key < iteratorNode.Keys[i] {
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i])
				break
			}

			// Travers pointer to the right of tree (key > fence pointer)
			if i == iteratorNode.numKeys-1 {
				iteratorNode = getNodeFromPageId(iteratorNode.Children[i+1])
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
		iteratorNode.Values[i] = value

		iteratorNode.numKeys++

		// Shift pointers
		iteratorNode.Children[iteratorNode.numKeys] = iteratorNode.Children[iteratorNode.numKeys-1]
		iteratorNode.Children[iteratorNode.numKeys-1] = 0

		iteratorNode.writeNodeToPage()
	} else // Current node has no space
	{
		var newLeaf = createNewNode(bpTree.Path)

		var copyKeys [MAX_BRANCHING_FACTOR + 2]int

		// Copy keys from current node
		copy(copyKeys[:MAX_BRANCHING_FACTOR], iteratorNode.Keys[:MAX_BRANCHING_FACTOR])

		// Find insertion point
		i := 0
		for key > copyKeys[i] && i < MAX_BRANCHING_FACTOR {
			i++
		}

		if key == iteratorNode.Keys[i] {
			return ErrSameKeyTwice
		}

		for j := MAX_BRANCHING_FACTOR + 1; j > i; j-- {
			copyKeys[j] = copyKeys[j-1]
		}

		copyKeys[i] = key
		L := (MAX_BRANCHING_FACTOR + 1) / 2
		iteratorNode.numKeys = L

		// Create new leaf
		newLeaf.IsLeaf = true
		newLeaf.Values[0] = value
		newLeaf.numKeys = MAX_BRANCHING_FACTOR + 1 - L

		// Point current node to new leaf
		iteratorNode.Children[iteratorNode.numKeys] = newLeaf.PageId

		newLeaf.Children[newLeaf.numKeys] = iteratorNode.Children[MAX_BRANCHING_FACTOR]
		iteratorNode.Children[MAX_BRANCHING_FACTOR] = 0

		copy(iteratorNode.Keys[:iteratorNode.numKeys], copyKeys[:iteratorNode.numKeys])

		k := iteratorNode.numKeys
		for i := 0; i < newLeaf.numKeys; i++ {
			newLeaf.Keys[i] = copyKeys[k]
			k++
		}

		newLeaf.writeNodeToPage()
		iteratorNode.writeNodeToPage()

		if iteratorNode.PageId == bpTree.RootPageId {
			var newRoot = createNewNode(bpTree.Path)

			newRoot.Keys[0] = newLeaf.Keys[0]
			newRoot.Children[0] = iteratorNode.PageId
			newRoot.Children[1] = newLeaf.PageId
			newRoot.IsLeaf = false
			newRoot.numKeys = 1
			bpTree.RootPageId = newRoot.PageId

			newRoot.writeNodeToPage()
		} else {
			internalInsertion(newLeaf.Keys[0], parent.PageId, newLeaf.PageId, bpTree)
		}
	}
	return nil
}

func createNewNode(path string) *Node {
	var newNode Node
	newNode.page = bufferPoolManager.NewPage(path)
	newNode.PageId = int(newNode.page.GetId())

	return &newNode
}

func internalInsertion(key int, iteratorPageId int, childPageId int, bpTree *BpTreeImpl) error {

	iterator := getNodeFromPageId(iteratorPageId)

	// Enough space to add a new key
	if iterator.numKeys < MAX_BRANCHING_FACTOR {
		i := 0
		for key > iterator.Keys[i] && i < iterator.numKeys {
			i++
		}

		// Make space for new key and pointer
		for j := iterator.numKeys; j > i; j-- {
			iterator.Keys[j] = iterator.Keys[j-1]
		}
		for j := iterator.numKeys + 1; j > i; j-- {
			iterator.Children[j] = iterator.Children[j-1]
		}

		iterator.Keys[i] = key
		iterator.numKeys++
		iterator.Children[i+1] = childPageId
		iterator.writeNodeToPage()

	} else { // Node is full, need to split
		var newNode = createNewNode(bpTree.Path)

		var copyKeys [MAX_BRANCHING_FACTOR + 1]int
		var copyChildren [MAX_BRANCHING_FACTOR + 2]int

		for i := 0; i < MAX_BRANCHING_FACTOR; i++ {
			copyKeys[i] = iterator.Keys[i]
		}

		for i := 0; i < MAX_BRANCHING_FACTOR+1; i++ {
			copyChildren[i] = iterator.Children[i]
		}

		i := 0
		for key > copyKeys[i] && i < MAX_BRANCHING_FACTOR {
			i++
		}

		for j := MAX_BRANCHING_FACTOR; j > i; j-- {
			copyKeys[j] = copyKeys[j-1]
		}
		copyKeys[i] = key

		for j := MAX_BRANCHING_FACTOR + 1; j > i; j-- {
			copyChildren[j] = copyChildren[j-1]
		}

		copyChildren[i+1] = childPageId
		newNode.IsLeaf = false
		var L = (MAX_BRANCHING_FACTOR + 1) / 2
		iterator.numKeys = L

		newNode.numKeys = MAX_BRANCHING_FACTOR - L

		var k = iterator.numKeys + 1
		for i := 0; i < newNode.numKeys; i++ {
			newNode.Keys[i] = copyKeys[k]
			k++
		}

		k = iterator.numKeys + 1
		for i := 0; i < newNode.numKeys+1; i++ {
			newNode.Children[i] = copyChildren[k]
			k++
		}

		newNode.writeNodeToPage()
		iterator.writeNodeToPage()

		if iterator.PageId == bpTree.RootPageId {
			var newRoot = createNewNode(bpTree.Path)

			newRoot.Keys[0] = iterator.Keys[iterator.numKeys]
			newRoot.Children[0] = iterator.PageId
			newRoot.Children[1] = newNode.PageId
			newRoot.IsLeaf = false
			newRoot.numKeys = 1
			bpTree.RootPageId = newRoot.PageId

			newRoot.writeNodeToPage()
		} else {
			internalInsertion(iterator.Keys[iterator.numKeys], findParent(bpTree.RootPageId, iterator.PageId).PageId, newNode.PageId, bpTree)
		}
	}

	return nil
}

func findParent(iteratorPageId int, childPageId int) *Node {
	var parent *Node
	iterator := getNodeFromPageId(iteratorPageId)
	child := getNodeFromPageId(childPageId)

	if iterator.IsLeaf || getNodeFromPageId(iterator.Children[0]).IsLeaf {
		return nil
	}

	for i := 0; i < iterator.numKeys+1; i++ {
		if iterator.Children[i] == child.PageId {
			parent = getNodeFromPageId(iterator.PageId)
			return parent
		} else {
			parent = findParent(iterator.Children[i], childPageId)
			if parent != nil {
				return parent
			}
		}
	}
	return parent
}

func (k *BpTreeImpl) Open(path string) (*BpTreeImpl, error) {
	return OpenKVStore(path)
}

// What should this do?
func (k *BpTreeImpl) Close() error {
	return nil
}

func (k *BpTreeImpl) DeleteStore(path string) error {
	return DeleteKVStore(path)
}

func CreateKVStore(btree BpTreeImpl) error {
	file, err := os.Create(btree.Path + "/KVSTORE")
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(btree)
	return err
}

func OpenKVStore(path string) (*BpTreeImpl, error) {
	file, err := os.Open(path + "/KVSTORE")
	defer file.Close()
	if err != nil {
		return nil, err
	}
	var btree BpTreeImpl
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&btree)
	return &btree, err
}

func DeleteKVStore(path string) error {
	err := os.RemoveAll(path + "/KVSTOREPAGES")
	if err != nil {
		return err
	}
	err = os.Remove(path + "/KVSTORE")
	return err
}
