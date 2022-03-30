package infrastructure

// DiskMaxNumPages sets the disk capacity
const DiskMaxNumPages = 1000 // Needlessly large for our case, because we assume disk space not an issue

// DiskManager responsible for interacting with disk
type DiskManager interface {
	ReadPage(PageID) (*Page, error)
	WritePage(*Page) error
	AllocatePage() *PageID
	DeallocatePage(PageID)
}
