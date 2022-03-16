package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	mem = 1 << (10 * 2)
)

/***
	Tests are named as follows:
	Test{function}_{scenario}_{expectation}
***/

func setupTestDB(path string, size int) (*kvImpl, error) {
	var kvImpl kvImpl
	return kvImpl.Create(path, size)
}

func closeTestDb(t *testing.T, kvImpl *kvImpl) {
	assert.Nil(t, kvImpl.Close())
}

func TestCreate(t *testing.T) {
	const defPath = "." // create in local directory

	_, err := setupTestDB(defPath, mem)

	assert.Equal(t, nil, err, "Creation failed")
}

func TestCreate_SizeTooBig_Fail(t *testing.T) {
	const memLarge = 1 << (10 * 4) // 10 GB > 1GB (MaxMem)

	kvImpl, err := setupTestDB(".", memLarge)
	defer closeTestDb(t, kvImpl)

	assert.EqualError(t, err, ErrOutOfRange.Error())
}

func TestCreate_NoPath_Fail(t *testing.T) {
	const invalidPath = ""

	kvImpl, err := setupTestDB(invalidPath, mem)
	defer closeTestDb(t, kvImpl)

	assert.EqualError(t, err, ErrInvalidPath.Error())
	assert.Nil(t, kvImpl.Close())
}

func TestOpen(t *testing.T) {
	const defPath = "." // create in local directory

	_, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	kvImpl, err := Open(defPath)
	assert.NotNil(t, kvImpl, "Unable to open KV store")
	assert.Nil(t, kvImpl.Close())
}

func TestDeleteStore(t *testing.T) {
	const defPath = "." // create in local directory

	kvImpl, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	err = kvImpl.DeleteStore(defPath)
	assert.Nil(t, err)
}

func TestDeleteStore_CannotBeOpened_Fail(t *testing.T) {
	const defPath = "." // create in local directory

	kvImpl, err := setupTestDB(defPath, mem)
	assert.Equal(t, nil, err, "Creation failed")

	err = DeleteStore(defPath)
	assert.Nil(t, err)

	_, err = Open(defPath)
	assert.EqualError(t, err, ErrInvalidPath.Error())
	assert.Nil(t, kvImpl.Close())
}

func TestDeleteStore_NoPath_Fail(t *testing.T) {
	const defPath = "." // create in local directory

	err := DeleteStore(defPath)
	assert.NotNil(t, err)
	assert.EqualError(t, err, ErrInvalidPath.Error())
}

func TestPut(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)

	err := kvImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
	assert.Nil(t, kvImpl.Close())
}

func TestPut_SameKeyTwice_Fails(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	kvImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	// Different value same key
	err := kvImpl.Put(123, [10]byte{1, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")

	assert.Nil(t, kvImpl.Close())
}

func TestGet(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	kvImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	value, err := kvImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.Equal(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was not present")
}

func TestGet_DeletedKey_Error(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	kvImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	kvImpl.Delete(123)

	value, err := kvImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.Equal(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was not present")
}

func TestDelete(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)

	err := kvImpl.Delete(123)

	assert.Equal(t, nil, err)
}

func TestDelete_DeleteTwice_Fail(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	kvImpl.Delete(123)

	err := kvImpl.Delete(123)

	assert.NotEqual(t, nil, err, "Deleting twice should raise an error")
}

func TestScan(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	val123 := [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}
	val127 := [10]byte{0, 0, 0, 0, 1, 0, 1, 0, 1, 0}
	val130 := [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
	kvImpl.Put(123, val123)
	kvImpl.Put(127, val127)
	kvImpl.Put(130, val130)

	results := kvImpl.ScanRange(122, 128)
	assert.Equal(t, len(results), 2, "Should have exactly two values in range")
	assert.Equal(t, results[0], val123, "Values should be ordered correctly")
	assert.Equal(t, results[1], val127, "Values should be ordered correctly")
}

func TestScan_TestRangeIncludesStarTExcludesEndOfRange(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)
	defer closeTestDb(t, kvImpl)
	val123 := [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}
	val127 := [10]byte{0, 0, 0, 0, 1, 0, 1, 0, 1, 0}
	val130 := [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
	kvImpl.Put(123, val123)
	kvImpl.Put(127, val127)
	kvImpl.Put(130, val130)

	results := kvImpl.ScanRange(123, 127)
	assert.Equal(t, len(results), 1, "Should have exactly on value in range")
	assert.Equal(t, results[0], val123, "Scan start should be included in result")
}
