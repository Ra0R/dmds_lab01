package infrastructure

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"strconv"
)

var (
	instance DiskManagerMock
)

// DiskManagerMock is a memory mock for disk manager
type DiskManagerMock struct {
	nextPageId int // tracks the number of pages ever allocated and serves as next pageId
	pages      map[PageID]*Page
	memMap     map[PageID]*os.File // Mocks disk;
}

// ReadPage reads a page from pages map
func (d *DiskManagerMock) ReadPage(pageID PageID) (*Page, error) {
	if file, ok := d.memMap[pageID]; ok {
		var page Page
		err := binary.Read(file, binary.LittleEndian, page)
		check(err)
		return &page, nil
	}
	if page, ok := d.pages[pageID]; ok {
		return page, nil
	}

	return nil, errors.New("page not found")
}

// WritePage writes a page in memory to pages
func (d *DiskManagerMock) WritePage(page *Page) error {
	d.pages[page.Id] = page
	var file = d.memMap[page.Id]

	err := binary.Write(file, binary.LittleEndian, page)
	return err
}

func asUint64(val interface{}) uint64 {
	ref := reflect.ValueOf(val)
	if ref.Kind() != reflect.Uint64 {
		return 0
	}
	return ref.Uint()
}

// AllocatePage allocates new page
func (d *DiskManagerMock) AllocatePage(path string) *PageID {
	if d.nextPageId == DiskMaxNumPages {
		return nil
	}
	pageID := PageID(d.nextPageId)
	d.nextPageId = d.nextPageId + 1

	err := os.MkdirAll(path+"/KVSTOREPAGES", os.ModePerm)
	file, err := os.Create(path + "/KVSTOREPAGES/" + strconv.Itoa(int(pageID)))
	check(err) // should never happen, pageID is unique
	d.memMap[pageID] = file

	return &pageID
}

// DeallocatePage removes page from disk
func (d *DiskManagerMock) DeallocatePage(pageID PageID) {
	delete(d.pages, pageID)
	delete(d.memMap, pageID)
}

// NewDiskManagerMock returns an empty disk manager mock. Should only use one unless wanting to mock multiple disks.
func NewDiskManagerMock() *DiskManagerMock {
	if instance.nextPageId < 1 {

		instance = DiskManagerMock{1, make(map[PageID]*Page), make(map[PageID]*os.File)} // <-- not thread safe
	}

	return &instance
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
