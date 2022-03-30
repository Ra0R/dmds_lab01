package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	mem = 1 << (10 * 2)
)

/***
	Tests are named as follows:
	Test{function}_{scenario}_{expectation}
***/

func setupTestDB(path string, size int) (*bpTreeImpl, error) {
	var bpTreeImpl bpTreeImpl
	return bpTreeImpl.Create(path, size)
}

func closeTestDb(t *testing.T, bpTreeImpl *bpTreeImpl) {
	assert.Nil(t, bpTreeImpl.Close())
}

func printBPTree(bpTree *bpTreeImpl, t *testing.T) {
	if bpTree.root == nil {
		fmt.Println("Empty tree.")
		return
	}

	printNode(bpTree.root, t)
}

func printNode(cursor *Node, t *testing.T) {

	if cursor != nil {
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
				// printNode(cursor.pointers[i], t)
			}
		}
	}
}

func TestCreate(t *testing.T) {
	const defPath = "." // create in local directory
	bpTree, err := setupTestDB(defPath, mem)

	printBPTree(bpTree, t)
	assert.Equal(t, nil, err, "Creation failed")
}

func TestCreate_SizeTooBig_Fail(t *testing.T) {
	const memLarge = 1 << (10 * 4) // 10 GB > 1GB (MaxMem)

	bpTreeImpl, err := setupTestDB(".", memLarge)
	defer closeTestDb(t, bpTreeImpl)

	assert.EqualError(t, err, ErrOutOfRange.Error())
}

func TestCreate_NoPath_Fail(t *testing.T) {
	const invalidPath = ""

	bpTreeImpl, err := setupTestDB(invalidPath, mem)
	defer closeTestDb(t, bpTreeImpl)

	assert.EqualError(t, err, ErrInvalidPath.Error())
	assert.Nil(t, bpTreeImpl.Close())
}

func TestOpen(t *testing.T) {
	const defPath = "." // create in local directory

	_, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	bpTreeImpl, err := Open(defPath)
	assert.NotNil(t, bpTreeImpl, "Unable to open KV store")
	assert.Nil(t, bpTreeImpl.Close())
}

func TestDeleteStore(t *testing.T) {
	const defPath = "." // create in local directory

	bpTreeImpl, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	err = bpTreeImpl.DeleteStore(defPath)
	assert.Nil(t, err)
}

func TestDeleteStore_CannotBeOpened_Fail(t *testing.T) {
	const defPath = "." // create in local directory

	bpTreeImpl, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	err = bpTreeImpl.DeleteStore(defPath)
	assert.Nil(t, err)

	_, err = Open(defPath)
	assert.EqualError(t, err, ErrInvalidPath.Error())
	assert.Nil(t, bpTreeImpl.Close())
}

func TestDeleteStore_NoPath_Fail(t *testing.T) {
	const defPath = "." // create in local directory
	bpTreeImpl, _ := setupTestDB(defPath, mem)

	err := bpTreeImpl.DeleteStore(defPath)
	assert.NotNil(t, err)
	assert.EqualError(t, err, ErrInvalidPath.Error())
}

func TestPut(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)

	err := bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	printBPTree(bpTreeImpl, t)
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
}

func TestPut_Benchmark(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)
	var i uint64
	i = 0
	var err error

	for ; i < 100; i++ {
		err = bpTreeImpl.Put(i, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	}
	printBPTree(bpTreeImpl, t)
	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
}

func TestPut_Insert_SplitNode(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	// defer closeTestDb(t, bpTreeImpl)

	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	bpTreeImpl.Put(22, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	bpTreeImpl.Put(1, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	err := bpTreeImpl.Put(59, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, bpTreeImpl.Close())
	printBPTree(bpTreeImpl, t)
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
}

func TestGet(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	//defer closeTestDb(t, bpTreeImpl)
	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	value, err := bpTreeImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.Equal(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was not present")
}

func TestGet_DeletedKey_Error(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)
	bpTreeImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	bpTreeImpl.Delete(123)

	value, err := bpTreeImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.NotEqual(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was present")
}

func TestDelete(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)

	err := bpTreeImpl.Delete(123)

	assert.Equal(t, nil, err)
}

func TestDelete_DeleteTwice_Fail(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)
	bpTreeImpl.Delete(123)

	err := bpTreeImpl.Delete(123)

	assert.NotEqual(t, nil, err, "Deleting twice should raise an error")
}

func TestScan(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)
	val123 := [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}
	val127 := [10]byte{0, 0, 0, 0, 1, 0, 1, 0, 1, 0}
	val130 := [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
	bpTreeImpl.Put(123, val123)
	bpTreeImpl.Put(127, val127)
	bpTreeImpl.Put(130, val130)

	results := bpTreeImpl.ScanRange(122, 128)
	assert.Equal(t, len(results), 2, "Should have exactly two values in range")
	assert.Equal(t, results[0], val123, "Values should be ordered correctly")
	assert.Equal(t, results[1], val127, "Values should be ordered correctly")
}

func TestScan_TestRangeIncludesStartExcludesEndOfRange(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, bpTreeImpl)
	val123 := [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}
	val127 := [10]byte{0, 0, 0, 0, 1, 0, 1, 0, 1, 0}
	val130 := [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
	bpTreeImpl.Put(123, val123)
	bpTreeImpl.Put(127, val127)
	bpTreeImpl.Put(130, val130)

	results := bpTreeImpl.ScanRange(123, 127)
	assert.Equal(t, len(results), 1, "Should have exactly on value in range")
	assert.Equal(t, results[0], val123, "Scan start should be included in result")
}

func Test_NodeToPage_PageToNode(t *testing.T) {
	var root Node

	root.IsLeaf = false
	var inode Inode
	inode.PageId = 5
	root.Children = [11]Inode{inode}

	data := root.serializeNode()

	fmt.Println(data)
	fmt.Println(len(data))
	var rootFromData Node
	rootFromData = *initializeNodeFromData(data)
	fmt.Println(root.Children)
	fmt.Println(rootFromData.Children)

	assert.Equal(t, root.PageId, rootFromData.PageId)
	assert.Equal(t, root.Children[0].PageId, rootFromData.Children[0].PageId)
	assert.Equal(t, root.IsLeaf, rootFromData.IsLeaf)
}

func Test_NodeToPageMultipleInodes_PageToNode(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	var root Node

	bpTreeImpl.root = &root
	bpTreeImpl.root.IsLeaf = false

	inode := Inode{1, nil}
	inode2 := Inode{2, nil}
	inode3 := Inode{3, nil}

	bpTreeImpl.root.Children = [11]Inode{inode, inode2, inode3}

	data := bpTreeImpl.root.serializeNode()

	fmt.Println(data)
	fmt.Println(len(data))
	var rootFromData Node
	rootFromData = *initializeNodeFromData(data)

	assert.Equal(t, root.PageId, rootFromData.PageId)
	assert.Equal(t, root.Children[0].PageId, rootFromData.Children[0].PageId)
	assert.Equal(t, root.IsLeaf, rootFromData.IsLeaf)
}

func Test_GetInodeSize(t *testing.T) {
	bpTreeImpl, _ := setupTestDB(".", mem)
	var root Node

	bpTreeImpl.root = &root
	bpTreeImpl.root.PageId = 99
	bpTreeImpl.root.IsLeaf = false

	inode := Inode{1, nil}
	inode2 := Inode{2, nil}

	bpTreeImpl.root.Children = [11]Inode{inode, inode2}
	len2 := len(bpTreeImpl.root.serializeNode())

	bpTreeImpl.root.Children[3] = Inode{3, nil}
	len1 := len(bpTreeImpl.root.serializeNode())

	inode_size := len1 - len2
	t.Log("Inode_size:", inode_size)
}

func ExampleMarshal() {
	type Item struct {
		Foo string
	}

	b, err := msgpack.Marshal(&Item{Foo: "bar"})
	if err != nil {
		panic(err)
	}

	var item Item
	err = msgpack.Unmarshal(b, &item)
	if err != nil {
		panic(err)
	}
	fmt.Println(item.Foo)
	// Output: bar
}

func TestEncode(t *testing.T) {
	var key uint64 = 12344
	var key2 uint64 = 1

	buf := &bytes.Buffer{}

	gob.NewEncoder(buf).Encode(key)
	fmt.Println(buf.Len())

	buf2 := &bytes.Buffer{}
	gob.NewEncoder(buf2).Encode(key2)
	fmt.Println(buf2.Len())
}
