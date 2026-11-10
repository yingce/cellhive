package dosupervisor

// hydratedPages tracks original SQLite pages without a map entry per page.
// The owning pagedFile mutex protects all accesses. Blocks are allocated only
// when touched; SQLite page numbers are one-based. Membership is never cleared:
// pagedFile.floor prevents hydration of truncated pages after file regrowth.
type hydratedPages struct {
	blocks map[uint32]*[16]uint64
	count  uint64
}

func (p *hydratedPages) has(page uint32) bool {
	if page == 0 {
		return false
	}
	bit := page - 1
	block := p.blocks[bit/1024]
	return block != nil && block[(bit%1024)/64]&(uint64(1)<<(bit%64)) != 0
}

func (p *hydratedPages) mark(page uint32) {
	if page == 0 {
		return
	}
	bit := page - 1
	block := p.blocks[bit/1024]
	if block == nil {
		if p.blocks == nil {
			p.blocks = make(map[uint32]*[16]uint64)
		}
		block = new([16]uint64)
		p.blocks[bit/1024] = block
	}
	word, mask := (bit%1024)/64, uint64(1)<<(bit%64)
	if block[word]&mask == 0 {
		block[word] |= mask
		p.count++
	}
}
