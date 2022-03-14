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
	ErrBadValue = errors.New(Package + "- bad value")
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
	Open(string) error
	Close() error
}

type kvImpl struct {
	MaxMem int
	Path   string
}

func (k kvImpl) Get(key uint64) ([]byte, error) {
	var retValue []byte
	return retValue, errors.New(Package + "- not implemented")
}

func (k *kvImpl) Create(path string, size int) (*KeyValueStore, error) {
	k.MaxMem = 1 << (10 * 3) // 1 GB
	k.Path = "."             // create in local directory

	if size > k.MaxMem || size == 0 {
		return nil, errors.New("'size' is out of range")
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}

func (k *kvImpl) Close() error {
	return errors.New(Package + " - not implemented")
}
