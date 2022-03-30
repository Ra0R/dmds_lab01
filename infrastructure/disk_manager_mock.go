package infrastructure

import "errors"

// DiskManagerMock is a memory mock for disk manager
type DiskManagerMock struct {
	nextPageId int // tracks the number of pages ever allocated and serves as next pageId
	pages      map[PageID]*Page
}

// ReadPage reads a page from pages map
func (d *DiskManagerMock) ReadPage(pageID PageID) (*Page, error) {
	if page, ok := d.pages[pageID]; ok {
		return page, nil
	}

	return nil, errors.New("page not found")
}

// WritePage writes a page in memory to pages
func (d *DiskManagerMock) WritePage(page *Page) error {
	d.pages[page.id] = page
	return nil
}

// AllocatePage allocates new page
func (d *DiskManagerMock) AllocatePage() *PageID {
	if d.nextPageId == DiskMaxNumPages {
		return nil
	}
	pageID := PageID(d.nextPageId)
	d.nextPageId = d.nextPageId + 1
	return &pageID
}

// DeallocatePage removes page from disk
func (d *DiskManagerMock) DeallocatePage(pageID PageID) {
	delete(d.pages, pageID)
}

// NewDiskManagerMock returns an empty disk manager mock. Should only use one unless wanting to mock multiple disks.
func NewDiskManagerMock() *DiskManagerMock {
	return &DiskManagerMock{0, make(map[PageID]*Page)}
}
