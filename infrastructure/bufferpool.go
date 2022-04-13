package infrastructure

import (
	"errors"
)

const PoolSize = 4 // Tiny size to facilitate testing

type BufferPoolManager struct {
	pages       [PoolSize]*Page // Pointers to every page – or nil if no page
	replacer    *ClockReplacer
	freeList    []FrameID          // List of all free frames
	pageTable   map[PageID]FrameID // Maps which page occupies which frame
	diskManager DiskManager
}

func (bufferPool *BufferPoolManager) FetchPage(pageID PageID) *Page {
	if frameID, ok := bufferPool.pageTable[pageID]; ok {
		page := bufferPool.pages[frameID]
		page.IncPinCount()
		(*bufferPool.replacer).Pin(frameID)
		return page
	}

	// get the id from free list or from replacer
	frameID, isFromFreeList := bufferPool.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := bufferPool.pages[*frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				bufferPool.diskManager.WritePage(currentPage)
			}

			delete(bufferPool.pageTable, currentPage.Id)
		}
	}

	page, err := bufferPool.diskManager.ReadPage(pageID)
	if err != nil {
		return nil
	}
	(*page).PinCounter = 1
	bufferPool.pageTable[pageID] = *frameID
	bufferPool.pages[*frameID] = page

	return page
}

func (bufferPool *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
	if frameID, ok := bufferPool.pageTable[pageID]; ok {
		page := bufferPool.pages[frameID]
		page.DecPinCount()

		if page.PinCounter <= 0 {
			(*bufferPool.replacer).Unpin(frameID)
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
func (bufferPool *BufferPoolManager) FlushPage(pageID PageID) bool {
	if frameID, ok := bufferPool.pageTable[pageID]; ok {
		page := bufferPool.pages[frameID]
		page.DecPinCount()

		bufferPool.diskManager.WritePage(page)
		page.isDirty = false

		return true
	}

	return false
}

// NewPage allocates a new page in the buffer pool with the disk manager help
func (bufferPool *BufferPoolManager) NewPage(path string) *Page {
	frameID, isFromFreeList := bufferPool.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := bufferPool.pages[*frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				bufferPool.diskManager.WritePage(currentPage)
			}

			delete(bufferPool.pageTable, currentPage.Id)
		}
	}

	// allocates new page
	pageID := bufferPool.diskManager.AllocatePage(path)
	if pageID == nil {
		return nil
	}
	page := &Page{*pageID, 1, true, []byte{}, path} //[PageSize]byte{}, do we need that?

	bufferPool.pageTable[*pageID] = *frameID
	bufferPool.pages[*frameID] = page

	return page
}

// DeletePage deletes a page from the buffer pool.
func (bufferPool *BufferPoolManager) DeletePage(pageID PageID) error {
	var frameID FrameID
	var ok bool
	if frameID, ok = bufferPool.pageTable[pageID]; !ok {
		return nil
	}

	page := bufferPool.pages[frameID]

	if page.PinCounter > 0 {
		return errors.New("pin count greater than 0, cannot delete page")
	}
	delete(bufferPool.pageTable, page.Id)
	(*bufferPool.replacer).Pin(frameID)
	bufferPool.diskManager.DeallocatePage(pageID)

	bufferPool.freeList = append(bufferPool.freeList, frameID)

	return nil
}

// FlushAllpages flushes all the pages in the buffer pool to disk.
func (bufferPool *BufferPoolManager) FlushAllpages() {
	for pageID := range bufferPool.pageTable {
		bufferPool.FlushPage(pageID)
	}
}

func (bufferPool *BufferPoolManager) getFrameID() (*FrameID, bool) {
	if len(bufferPool.freeList) > 0 {
		frameID, newFreeList := bufferPool.freeList[0], bufferPool.freeList[1:]
		bufferPool.freeList = newFreeList

		return &frameID, true
	}

	return (*bufferPool.replacer).ChooseVictim(), false
}

// NewBufferPoolManager Empty buffer pool manager
func NewBufferPoolManager(DiskManager DiskManager, clockReplacer *ClockReplacer) *BufferPoolManager {
	freeList := make([]FrameID, 0)
	pages := [PoolSize]*Page{}
	for i := 0; i < PoolSize; i++ {
		freeList = append(freeList, FrameID(i))
		pages[FrameID(i)] = nil
	}
	return &BufferPoolManager{pages, clockReplacer, freeList, make(map[PageID]FrameID), DiskManager}
}
