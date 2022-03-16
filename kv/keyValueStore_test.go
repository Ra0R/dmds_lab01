package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	mem = 1 << (10 * 2)
)

func setupTestDB(path string, size int) (*kvImpl, error) {
	var kvImpl kvImpl
	return kvImpl.Create(path, size)
}
func TestCreate(t *testing.T) {
	const defPath = "." // create in local directory

	_, err := setupTestDB(defPath, mem)

	assert.Equal(t, nil, err, "Creation failed")
}

func TestCreate_SizeTooBig_Fail(t *testing.T) {
	const memLarge = 1 << (10 * 4) // 10 GB > 1GB (MaxMem)

	_, err := setupTestDB(".", memLarge)

	assert.EqualError(t, err, ErrOutOfRange.Error())
}

func TestCreate_NoPath_Fail(t *testing.T) {
	const invalidPath = ""

	_, err := setupTestDB(invalidPath, mem)

	assert.EqualError(t, err, ErrInvalidPath.Error())
}

func TestPut(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)

	err := kvImpl.Put(123, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1})

	assert.Equal(t, nil, err, "Insert failed")
}

func TestGet(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)

	value, err := kvImpl.Get(123)

	assert.Equal(t, err, nil, "An error occured while getting key")
	assert.Equal(t, [10]byte{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, value, "Key was not present")
}

func TestDelete(t *testing.T) {
	kvImpl, _ := setupTestDB(".", mem)

	err := kvImpl.Delete(123)

	assert.Equal(t, nil, err)
}

func TestScan(t *testing.T) {
	t.Errorf("Test not implemented")
}

//TODO Test edge cases: empty key, empty value

//TODO? Tests for memory allocation/overflow
