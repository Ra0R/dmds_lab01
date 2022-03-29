package infrastructure

type PageID int

const pageSize = 100 // 4kb page size, classic
var nextPageId = 1

// Page represents a page on disk
type Page struct {
	id         PageID
	pinCounter int // number of times page has been pinned
	isDirty    bool
	data       [pageSize]byte
	//writeLock  ignored for now
	//isLeaf     bool part of data
}

func createNewPage() Page {
	//	Make sure pageId is unique
}

func (p *Page) getId() PageID {
	return p.id
}

func (p *Page) getPinCount() int {
	return p.pinCounter
}

func (p *Page) incPinCount() {
	p.pinCounter++
}

func (p *Page) DecPinCount() {
	if p.pinCounter > 0 {
		p.pinCounter--
	}
}
