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

type keyValueStore interface {
	// Maybe we need to pass a reference to the store aswell? //https://github.com/akrylysov/pogreb/blob/master/db.go
	Get(uint64) ([]byte, error)
	Put(uint64, [10]byte) error
	Delete(uint64) error
	ScanRange(uint64, uint64)

	// Controll interface
	// either on memory or storage
	// Needs path and size
	Create(string, int) (*keyValueStore, error)

	// Opens KV store at given path
	Open(string) error

	Close(*keyValueStore) error
}

// TODO

func Get(key uint64) ([]byte, error) {
	var retValue []byte
	return retValue, errors.New(Package + "- not implemented")
}

const MaxMem = 1 << (10 * 3) // 1 GB
const defPath = "."          // create in local directory

func Create(path string, size int) (*keyValueStore, error) {
	if size > MaxMem || size == 0 {
		return nil, errors.New("'size' is out of range")
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}
