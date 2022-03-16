package kv

import (
	"errors"
	"fmt"
)

var (
	Package = "kv"
	// ErrNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrNotFound = errors.New(Package + "- key not found")

	// ErrBadValue is returned when the value supplied to the Put method is invalid
	ErrBadValue    = errors.New(Package + "- bad value")
	ErrOutOfRange  = errors.New(Package + "- size' is out of range")
	ErrInvalidPath = errors.New(Package + "- 'path' is not valid")
)

type KeyValueStore interface {
	Get(uint64) ([]byte, error)
	Put(uint64, [10]byte) error // Might return an error on inserting same key twice
	Delete(uint64) error
	ScanRange(uint64, uint64)

	// Control interface
	// Path and Size
	Create(string, int) (KeyValueStore, error)

	// Opens KV store at given path
	Open(string) (KeyValueStore, error)
	Close() error
}

type kvImpl struct {
	MaxMem int
	Path   string
}

func (k kvImpl) Get(key uint64) ([]byte, error) {
	var retValue []byte
	return retValue, nil
}

func (k *kvImpl) Put(key uint64, value [10]byte) error {
	return nil
}

func (k *kvImpl) Delete(key uint64) error {
	return nil
}

func (k *kvImpl) Create(path string, size int) (*kvImpl, error) {
	k.MaxMem = 1 << (10 * 3) // 1 GB
	k.Path = "."             // create in local directory

	if size > k.MaxMem || size == 0 {
		return nil, ErrOutOfRange
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, ErrInvalidPath
	}
	return nil, nil
}

func Open(path string) (*kvImpl, error) {
	return nil, nil
}

func (k *kvImpl) Close() error {
	return nil
}
