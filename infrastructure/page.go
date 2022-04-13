package infrastructure

type PageID int

const PageSize int = 1000 // Relatively small size, facilitates testing. For production, os.Getpagesize() would be optimal.

// Page represents a page on disk
type Page struct {
	Id         PageID
	PinCounter int // number of times page has been pinned
	isDirty    bool
	Data       []byte
	Path       string
	// writeLock  concurrency ignored
}

func (p *Page) SetData(data []byte) {
	p.isDirty = true
	p.Data = data
}

func (p *Page) GetData() []byte {
	return p.Data
}

func (p *Page) SetId(pageId PageID) {
	p.Id = pageId
}

func (p *Page) GetId() PageID {
	return p.Id
}

func (p *Page) GetPinCount() int {
	return p.PinCounter
}

func (p *Page) IncPinCount() {
	p.PinCounter++
}

func (p *Page) DecPinCount() {
	if p.PinCounter > 0 {
		p.PinCounter--
	}
}
