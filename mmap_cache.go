package lsvd

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/edsrzf/mmap-go"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sys/unix"
)

/*

mmap cache implements a read cache system via an arc cache and a mmap'd
file. We mmap the file to it's max cache size, then we begin writing blocks
there as needed directly in memory.

The arc cache holds the LBA => offset mapping, where we just bump the offset
until we reach the end. At which time, the arc cache starts to expire entries
and a freelist is automatically built when entries are purged.

So if there are offsets in the freelist, use them. otherwise bump the offset.

Another option is to fully populate the freelist like btree. In this method,
we allocate N blocks at the beginning to store a complete list of all the offsets.

*/

type DiskCache struct {
	path   string
	blocks int

	blocksOffset int64

	f *os.File
	m mmap.MMap

	nextOffset int64
	nextEntry  uint32
	left       int

	freelist []uint32
	lru      *lru.Cache[LBA, uint32]
}

const entrySize = 12

func NewDiskCache(path string, blocks int) (*DiskCache, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	freelist := make([]uint32, 0)

	l, err := lru.NewWithEvict[LBA, uint32](blocks, func(key LBA, value uint32) {
		freelist = append(freelist, value)
	})
	if err != nil {
		return nil, err
	}

	cacheHeader := int64(blocks) * entrySize

	sz := cacheHeader + (int64(blocks) * BlockSize)

	err = f.Truncate(sz)
	if err != nil {
		f.Close()
		return nil, err
	}

	m, err := mmap.MapRegion(f, int(sz), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED, 0)
	if err != nil {
		f.Close()
		return nil, err
	}

	dc := &DiskCache{
		path:         path,
		blocks:       blocks,
		blocksOffset: cacheHeader,

		f: f,
		m: m,

		nextOffset: cacheHeader,
		nextEntry:  0,
		left:       blocks,

		lru: l,
	}

	return dc, nil
}

func (d *DiskCache) Close() error {
	d.m.Flush()
	d.m.Unmap()
	return d.f.Close()
}

var ErrCacheMiss = errors.New("cache miss")

func (d *DiskCache) ReadBlock(lba LBA, block []byte) error {
	defer d.m.Flush()

	off, ok := d.lru.Get(lba)
	if ok {
		dataOff := binary.BigEndian.Uint32(d.m[off+8:])
		copy(block, d.m[dataOff:])
		return nil
	}

	h := d.m

	for i := 0; i < d.blocks; i++ {
		ent := LBA(binary.BigEndian.Uint64(h))
		off := binary.BigEndian.Uint32(h[8:])

		if lba == ent {
			d.lru.Add(ent, off)
			copy(block, d.m[off:])
			return nil
		}

		h = h[12:]
	}

	return ErrCacheMiss
}

func (d *DiskCache) scanForEntry(lba LBA) (uint32, bool) {
	h := d.m

	for i := 0; i < d.blocks; i++ {
		ent := LBA(binary.BigEndian.Uint64(h))
		off := binary.BigEndian.Uint32(h[8:])

		if lba == ent {
			return off, true
		}

		h = h[12:]
	}

	return 0, false
}

func (d *DiskCache) WriteBlock(lba LBA, block []byte) error {
	off, ok := d.lru.Get(lba)
	if !ok {
		off, ok = d.scanForEntry(lba)
		if ok {
			d.lru.Add(lba, off)
		}
	}

	if ok {
		copy(d.m[off:], block)
		return nil
	}

	var (
		pos     []byte
		dataPos uint32
	)

	if d.lru.Len() == d.blocks {
		_, off, _ = d.lru.RemoveOldest()

		pos = d.m[off:]

		dataPos = binary.BigEndian.Uint32(pos[8:])
	} else {
		off = d.nextEntry

		// Use the logic to just place it round-robin
		pos = d.m[off:]

		dataPos = uint32(d.nextOffset)

		d.nextEntry = d.nextEntry + 12
		d.nextOffset += BlockSize

		d.left--

		if d.left == 0 {
			d.left = d.blocks
			d.nextEntry = 0
			d.nextOffset = d.blocksOffset
		}
	}

	d.lru.Add(lba, off)

	binary.BigEndian.PutUint64(pos, uint64(lba))
	binary.BigEndian.PutUint32(pos[8:], uint32(dataPos))

	copy(d.m[dataPos:], block)

	return nil
}
