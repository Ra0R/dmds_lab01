package infrastructure

type PageID int

const PageSize int = 1000

var nextPageId = 1

// Page represents a page on disk
type Page struct {
	id         PageID
	pinCounter int // number of times page has been pinned
	isDirty    bool
	data       []byte
	//writeLock  ignored for now
}

func createNewPage() Page {
	//	Make sure pageId is unique
	var page Page

	return page
}

func (p *Page) SetData(data *[]byte) {
	p.isDirty = true
	p.data = *data
}

func (p *Page) GetData() *[]byte {
	return &p.data
}

func (p *Page) SetId(pageId PageID) {
	p.id = pageId
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
