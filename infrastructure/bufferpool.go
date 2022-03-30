package infrastructure

import (
	"errors"
)

const PoolSize = 2

type BufferPoolManager struct {
	pages       [PoolSize]*Page
	replacer    *ClockReplacer
	freeList    []FrameID
	pageTable   map[PageID]FrameID
	diskManager DiskManager
}

// FetchPage fetches the requested page from the buffer pool.
func (b *BufferPoolManager) FetchPage(pageID PageID) *Page {
	// if it is on buffer pool return it
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.pinCounter++
		(*b.replacer).Pin(frameID)
		return page
	}

	// get the id from free list or from replacer
	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := b.pages[*frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				b.diskManager.WritePage(currentPage)
			}

			delete(b.pageTable, currentPage.id)
		}
	}

	page, err := b.diskManager.ReadPage(pageID)
	if err != nil {
		return nil
	}
	(*page).pinCounter = 1
	b.pageTable[pageID] = *frameID
	b.pages[*frameID] = page

	return page
}

// UnpinPage unpins the target page from the buffer pool.
func (b *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.DecPinCount()

		if page.pinCounter <= 0 {
			(*b.replacer).Unpin(frameID)
		}

		if page.isDirty || isDirty {
			page.isDirty = true
		} else {
			page.isDirty = false
		}

		return nil
	}

	return errors.New("Could not find page")
}

// FlushPage Flushes the target page to disk.
func (b *BufferPoolManager) FlushPage(pageID PageID) bool {
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.DecPinCount()

		b.diskManager.WritePage(page)
		page.isDirty = false

		return true
	}

	return false
}

// NewPage allocates a new page in the buffer pool with the disk manager help
func (b *BufferPoolManager) NewPage() *Page {
	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := b.pages[*frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				b.diskManager.WritePage(currentPage)
			}

			delete(b.pageTable, currentPage.id)
		}
	}

	// allocates new page
	pageID := b.diskManager.AllocatePage()
	if pageID == nil {
		return nil
	}
	page := &Page{*pageID, 1, false, []byte{}} //[PageSize]byte{}, do we need that?

	b.pageTable[*pageID] = *frameID
	b.pages[*frameID] = page

	return page
}

// DeletePage deletes a page from the buffer pool.
func (b *BufferPoolManager) DeletePage(pageID PageID) error {
	var frameID FrameID
	var ok bool
	if frameID, ok = b.pageTable[pageID]; !ok {
		return nil
	}

	page := b.pages[frameID]

	if page.pinCounter > 0 {
		return errors.New("Pin count greater than 0")
	}
	delete(b.pageTable, page.id)
	(*b.replacer).Pin(frameID)
	b.diskManager.DeallocatePage(pageID)

	b.freeList = append(b.freeList, frameID)

	return nil

}

// FlushAllpages flushes all the pages in the buffer pool to disk.
func (b *BufferPoolManager) FlushAllpages() {
	for pageID := range b.pageTable {
		b.FlushPage(pageID)
	}
}

func (b *BufferPoolManager) getFrameID() (*FrameID, bool) {
	if len(b.freeList) > 0 {
		frameID, newFreeList := b.freeList[0], b.freeList[1:]
		b.freeList = newFreeList

		return &frameID, true
	}

	return (*b.replacer).Victim(), false
}

//NewBufferPoolManager returns a empty buffer pool manager
func NewBufferPoolManager(DiskManager DiskManager, clockReplacer *ClockReplacer) *BufferPoolManager {
	freeList := make([]FrameID, 0)
	pages := [PoolSize]*Page{}
	for i := 0; i < PoolSize; i++ {
		freeList = append(freeList, FrameID(i))
		pages[FrameID(i)] = nil
	}
	return &BufferPoolManager{pages, clockReplacer, freeList, make(map[PageID]FrameID), DiskManager}
}
