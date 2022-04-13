package kv

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	mem int = 1 << (10 * 2)
)

/***
	Tests are named as follows:
	Test{function}_{scenario}_{expectation}
***/

func setupTestDB(path string, size int) (*BpTreeImpl, error) {
	var bpTreeImpl BpTreeImpl
	bpTreeImpl.Path = "."
	bpTreeImpl.Create(path, size)
	return bpTreeImpl.Open(path)
}

func closeTestDb(t *testing.T, bpTreeImpl *BpTreeImpl) {
	assert.Nil(t, bpTreeImpl.Close())
}

func printBPTree(bpTree *BpTreeImpl, t *testing.T) {
	rootNode := getNodeFromPageId(bpTree.RootPageId)
	if rootNode == nil {
		fmt.Println("Empty tree.")
		return
	}

	printNode(rootNode, t)
}

func printNode(cursor *Node, t *testing.T) {

	if cursor != nil {
		fmt.Print("PageId:[", cursor.PageId)
		fmt.Print("] --- ")
		// Print keys
		fmt.Print("[")
		for i := 0; i < cursor.numKeys; i++ {
			fmt.Print(cursor.Keys[i], " | ")
		}
		fmt.Print("]")
		fmt.Println()
		// Print Values for Leaf Nodes
		if !cursor.IsLeaf {
			for i := 0; i < cursor.numKeys+1; i++ {
				printNode(getNodeFromPageId(cursor.Children[i]), t)
			}
		}
	}
}

func TestCreate(t *testing.T) {
	const defPath = "." // create in local directory
	bpTree, err := setupTestDB(defPath, mem)

	assert.Equal(t, nil, err, "Creation failed")
	bpTree.DeleteStore(defPath)
}

func TestCreate_NoPath_Fails(t *testing.T) {
	const invalidPath = ""

	bpTreeImpl, err := setupTestDB(invalidPath, mem)

	assert.Nil(t, bpTreeImpl, "Creation failed")
	assert.NotNil(t, err, "Creation failed")
	bpTreeImpl.DeleteStore(".")
}

func TestOpen(t *testing.T) {
	const defPath = "." // create in local directory

	_, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	bpTreeImpl, err := OpenKVStore(defPath)
	assert.NotNil(t, bpTreeImpl, "Unable to open KV store")
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}

func TestDeleteStore(t *testing.T) {
	const defPath = "." // create in local directory

	bpTreeImpl, err := setupTestDB(defPath, mem)

	err = bpTreeImpl.DeleteStore(defPath)
	assert.Nil(t, err)
}

func TestDeleteStore_CannotBeOpened_Fail(t *testing.T) {
	const defPath = "." // create in local directory

	bpTreeImpl, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	err = bpTreeImpl.DeleteStore(defPath)
	assert.Nil(t, err)

	_, err = OpenKVStore(defPath)
	assert.NotNil(t, err)
	assert.Nil(t, bpTreeImpl.Close())
}

func TestPut(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)

	err := bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}

func TestPut_ComplexInsert(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)

	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	bpTreeImpl.Put(22, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	err := bpTreeImpl.Put(1, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	printBPTree(bpTreeImpl, t)
	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}

func TestPut_Benchmark(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)
	var i int
	i = 0
	var err error

	for ; i < 16; i++ {
		err = bpTreeImpl.Put(i, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	}
	printBPTree(bpTreeImpl, t)
	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}
func TestPut_InsertMultiple(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)
	for i := 1; i < 8; i++ {
		bpTreeImpl.Put((int)(i), [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	}

	printBPTree(bpTreeImpl, t)
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}
func TestPut_Insert_SplitNode(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)
	for i := 1; i < 12; i++ {
		bpTreeImpl.Put((int)(i), [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
		printBPTree(bpTreeImpl, t)
	}

	err := bpTreeImpl.Put(12, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
	printBPTree(bpTreeImpl, t)
	assert.Nil(t, bpTreeImpl.Close())
	bpTreeImpl.DeleteStore(".")
}

func TestPut_SameKeyTwice_Fails(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)
	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	// Different value same key
	err := bpTreeImpl.Put(123, [10]byte{1, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.EqualError(t, err, ErrSameKeyTwice.Error(), "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	printBPTree(bpTreeImpl, t)
	bpTreeImpl.DeleteStore(".")
}

func TestGet(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	//defer closeTestDb(t, bpTreeImpl)
	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	value, err := bpTreeImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.Equal(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was not present")
	bpTreeImpl.DeleteStore(".")
}

func Test_NodeToPage_PageToNode(t *testing.T) {
	var root Node

	root.IsLeaf = false
	root.PageId = 5
	root.ParentPageId = 10
	root.NextPageId = 15
	root.numKeys = 1
	root.Children[0] = 5

	data := root.serializeNode()

	fmt.Println(data)
	fmt.Println(len(data))
	var rootFromData Node
	rootFromData = *initializeNodeFromData(data)
	fmt.Println(root.Children)
	fmt.Println(rootFromData.Children)

	assert.Equal(t, root.PageId, rootFromData.PageId)
	assert.Equal(t, root.ParentPageId, rootFromData.ParentPageId)
	assert.Equal(t, root.NextPageId, rootFromData.NextPageId)
	assert.Equal(t, root.numKeys, rootFromData.numKeys)

	assert.Equal(t, root.Children[0], rootFromData.Children[0])
	assert.Equal(t, root.IsLeaf, rootFromData.IsLeaf)
}

func Test_NodeToPageMultipleInodes_PageToNode(t *testing.T) {
	var root Node
	root.IsLeaf = false
	root.numKeys = 3
	root.Children[0] = 1
	root.Children[1] = 2
	root.Children[2] = 3
	data := root.serializeNode()

	fmt.Println(data)
	fmt.Println(len(data))
	var rootFromData Node
	rootFromData = *initializeNodeFromData(data)

	assert.Equal(t, root.PageId, rootFromData.PageId)
	assert.Equal(t, root.Children[0], rootFromData.Children[0])
	assert.Equal(t, root.Children[1], rootFromData.Children[1])
	assert.Equal(t, root.Children[2], rootFromData.Children[2])
	assert.Equal(t, root.IsLeaf, rootFromData.IsLeaf)
}

func Test_NodeToPageMultipleKeyValue_PageToNode(t *testing.T) {
	var root Node

	root.IsLeaf = true
	root.numKeys = 1
	val1 := [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
	val2 := [10]byte{1, 0, 0, 0, 1, 1, 1, 1, 1, 1}

	root.numKeys = 2
	root.Keys[0] = 9
	root.Values[0] = val1
	root.Keys[1] = 2
	root.Values[1] = val2
	data := root.serializeNode()

	fmt.Println(data)
	fmt.Println(len(data))
	var rootFromData Node
	rootFromData = *initializeNodeFromData(data)

	assert.Equal(t, root.PageId, rootFromData.PageId)
	assert.Equal(t, root.Keys[0], rootFromData.Keys[0])
	assert.Equal(t, root.Values[0], rootFromData.Values[0])
	assert.Equal(t, root.Keys[1], rootFromData.Keys[1])
	assert.Equal(t, root.Values[1], rootFromData.Values[1])
	assert.Equal(t, root.IsLeaf, rootFromData.IsLeaf)
}

// Unused
func TestPageSize(t *testing.T) {

	t.Log(os.Getpagesize())
}
