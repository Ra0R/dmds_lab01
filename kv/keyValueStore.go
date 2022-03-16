package kv

import (
	"errors"
	"fmt"
)

var (
	Package = "kv"
	// ErrNotFound is returned when the key supplied to a Get or Delete method does not exist in the database or the
	// store does not exist at the given path.
	ErrNotFound = errors.New(Package + " - key not found")

	// ErrBadValue is returned when the value supplied to the Put method is invalid
	ErrBadValue = errors.New(Package + " - bad value")

	// ErrorOutOfRange is returned when the supplied size is too large
	ErrOutOfRange = errors.New(Package + " - size' is out of range")

	// ErrInvalidPath is returned when the path that has been given is not valid (inexistent/not writable)
	ErrInvalidPath = errors.New(Package + " - 'path' is not valid")
)

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

// ScanRange Returns all values with keys ranging [begin, end) (i.e. range with begin, but without end)
func (k *kvImpl) ScanRange(begin uint64, end uint64) [][10]byte {
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

func DeleteStore(path string) error {
	return nil
}
