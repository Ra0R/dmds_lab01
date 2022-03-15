package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	const mem = 1 << (10 * 2) // .1 GB
	const defPath = "."       // create in local directory

	var kvImpl kvImpl

	_, err := kvImpl.Create(defPath, mem)

	if err != nil {
		t.Fatalf("Creation failed")
	}
}

func TestCreate_SizeTooBig_Fail(t *testing.T) {
	const mem = 1 << (10 * 4) // 10 GB > 1GB (MaxMem)

	var kvImpl kvImpl

	_, err := kvImpl.Create(".", mem)
	assert.EqualError(t, err, ErrOutOfRange.Error())
}

func TestPut(t *testing.T) {
	t.Errorf("Test not implemented")
}

func TestGet(t *testing.T) {
	t.Errorf("Test not implemented")

}

func TestScan(t *testing.T) {
	t.Errorf("Test not implemented")
}

//TODO? Tests for memory allocation/overflow
