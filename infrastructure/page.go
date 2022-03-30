package infrastructure

type PageID int

const PageSize = 100 // 4kb page size, classic
var nextPageId = 1

// Page represents a page on disk
type Page struct {
	id         PageID
	pinCounter int // number of times page has been pinned
	isDirty    bool
	Data       [PageSize]byte
	//writeLock  ignored for now
	//isLeaf     bool part of data
}

func createNewPage() Page {
	//	Make sure pageId is unique
	var page Page

	return page
}

func (p *Page) GetId() PageID {
	return p.id
}

func (p *Page) GetPinCount() int {
	return p.pinCounter
}

func (p *Page) IncPinCount() {
	p.pinCounter++
}

func (p *Page) DecPinCount() {
	if p.pinCounter > 0 {
		p.pinCounter--
	}
}
