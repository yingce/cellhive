package replica

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
)

// ErrPageIndexCapacity is admission failure, never evidence of corrupted data.
var ErrPageIndexCapacity = errors.New("replica: page index resident budget exhausted")

// Includes construction scratch space as well as retained indexes. Reservations
// precede allocation; actual Go allocator overhead is covered conservatively.
const pageIndexResidentLimit int64 = 512 << 20

var pageIndexResident atomic.Int64

func reservePageIndex(n int64) error {
	if n < 0 || n > pageIndexResidentLimit {
		return ErrPageIndexCapacity
	}
	for {
		old := pageIndexResident.Load()
		if old > pageIndexResidentLimit-n {
			return ErrPageIndexCapacity
		}
		if pageIndexResident.CompareAndSwap(old, old+n) {
			return nil
		}
	}
}
func (pf *PageFetcher) reserve(n int64) error {
	if err := reservePageIndex(n); err != nil {
		return err
	}
	pf.resident += n
	return nil
}
func (pf *PageFetcher) ResidentBytes() int64 { return pf.resident }

// WriteLocalIndex serializes the exact immutable cut, including ETags. It never
// consults remote metadata. The caller owns synchronization and atomic publication.
func (pf *PageFetcher) WriteLocalIndex(w io.Writer) error {
	put := func(v any) error { return binary.Write(w, binary.LittleEndian, v) }
	for _, v := range []any{uint64(0x314958444e494843), pf.pageSize, pf.commit, pf.watermark, uint32(len(pf.objects)), uint32(len(pf.pages.blocks)), pf.pages.count} {
		if err := put(v); err != nil {
			return err
		}
	}
	for _, o := range pf.objects {
		for _, s := range []string{o.key, o.etag} {
			if len(s) > 65536 {
				return fmt.Errorf("replica: local index identity too long")
			}
			if err := put(uint32(len(s))); err != nil {
				return err
			}
			if _, err := io.WriteString(w, s); err != nil {
				return err
			}
		}
	}
	for n, b := range pf.pages.blocks {
		if err := put(n); err != nil {
			return err
		}
		for _, loc := range b {
			for _, v := range []any{loc.Offset, loc.Size, loc.object} {
				if err := put(v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// DropLocalIndex releases payload after its durable local descriptor is published.
// It retains only the fixed transport and scalar immutable cut identity.
func (pf *PageFetcher) DropLocalIndex() {
	if pf == nil {
		return
	}
	pf.pages = pageIndex{}
	pf.objects = nil
	pageIndexResident.Add(-pf.resident)
	pf.resident = 0
}

func (pf *PageFetcher) ReadLocalIndex(r io.Reader) (err error) {
	if pf.resident != 0 || len(pf.pages.blocks) != 0 || len(pf.objects) != 0 {
		return errors.New("replica: index already resident")
	}
	defer func() {
		if err != nil {
			pf.DropLocalIndex()
		}
	}()
	get := func(v any) error { return binary.Read(r, binary.LittleEndian, v) }
	var magic, watermark, count uint64
	var ps, commit, objects, blocks uint32
	for _, v := range []any{&magic, &ps, &commit, &watermark, &objects, &blocks, &count} {
		if err = get(v); err != nil {
			return err
		}
	}
	if magic != 0x314958444e494843 || ps != pf.pageSize || commit != pf.commit || watermark != pf.watermark || count > uint64(commit) || objects > 16384 || blocks > 4194304 {
		return errors.New("replica: mismatched local immutable cut")
	}
	// Charge map/array overhead and object slots before allocating either.
	if err = pf.reserve(int64(blocks)*(pageIndexBlockSize*16+128) + int64(objects)*64 + int64(blocks+1)*256); err != nil {
		return err
	}
	pf.pages.blocks = make(map[uint32]*[pageIndexBlockSize]pageLocation, blocks)
	pf.objects = make([]pageObject, objects)
	for i := range pf.objects {
		dst := []*string{&pf.objects[i].key, &pf.objects[i].etag}
		for _, s := range dst {
			var n uint32
			if err = get(&n); err != nil {
				return err
			}
			if n > 65536 {
				return errors.New("replica: invalid local object identity")
			}
			if err = pf.reserve(int64(n)); err != nil {
				return err
			}
			var b strings.Builder
			b.Grow(int(n))
			if _, err = io.CopyN(&b, r, int64(n)); err != nil {
				return err
			}
			*s = b.String()
		}
	}
	for i := uint32(0); i < blocks; i++ {
		var n uint32
		if err = get(&n); err != nil {
			return err
		}
		if _, ok := pf.pages.blocks[n]; ok || uint64(n) > uint64(commit)/pageIndexBlockSize {
			return errors.New("replica: duplicate or invalid local block")
		}
		b := new([pageIndexBlockSize]pageLocation)
		for j := range b {
			for _, v := range []any{&b[j].Offset, &b[j].Size, &b[j].object} {
				if err = get(v); err != nil {
					return err
				}
			}
			if b[j].Size != 0 {
				if b[j].object >= objects || uint64(n)*pageIndexBlockSize+uint64(j) > uint64(commit) {
					return errors.New("replica: invalid local page location")
				}
				pf.pages.count++
			}
		}
		pf.pages.blocks[n] = b
	}
	if pf.pages.count != count {
		return errors.New("replica: local page count mismatch")
	}
	return nil
}
