package infrastructure

type PageID int

const pageSize = 4096 // 4kb page size, classic

// Page represents a page on disk
type Page struct {
	id         PageID
	pinCounter int // number of times page has been pinned
	isDirty    bool
	data       [pageSize]byte
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
