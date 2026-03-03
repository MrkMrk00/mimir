// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"go.uber.org/atomic"
)

const (
	offsetCatalogueVersion = 1

	// Special key that holds the watermark of the head block.
	offsetCatalogueBlockHead = "__head__"
)

type offsetWatermark struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (o offsetWatermark) String() string {
	return fmt.Sprintf("%d/%d", o.Partition, o.Offset)
}

type offsetCatalogueData struct {
	Version int                        `json:"version"`
	Data    map[string]offsetWatermark `json:"data"`
}

// offsetCatalogue tracks the mapping of block ULID (or __head__) to the Kafka offset watermark
// at the time the TSDB was compacted.
type offsetCatalogue struct {
	logger log.Logger
	dir    string
	userID string

	mu    sync.Mutex
	data  map[string]offsetWatermark
	dirty bool
}

func newOffsetCatalogue(logger log.Logger, dir, userID string) *offsetCatalogue {
	return &offsetCatalogue{
		logger: logger,
		dir:    dir,
		userID: userID,
		data:   make(map[string]offsetWatermark),
	}
}

const offsetCatalogueFilename = "offset-catalogue.json"

func (c *offsetCatalogue) filePath() string {
	return filepath.Join(c.dir, offsetCatalogueFilename)
}

func (c *offsetCatalogue) Load() error {
	// TODO: not implemented
	return nil
}

//func (c *offsetCatalogue) Prune(existingBlocks []ulid.ULID) {
//	// Prune removes entries for blocks not in existingBlocks. The __head__ entry is always kept.
//	panic("not implemented")
//
//}
//
//func (c *offsetCatalogue) Save() error {
//	// Save persists the catalogue to disk atomically. No-op if nothing changed.
//	panic("not implemented")
//}

func (c *offsetCatalogue) GetHead() (offsetWatermark, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wm, ok := c.data[offsetCatalogueBlockHead]
	return wm, ok
}

func (c *offsetCatalogue) SetHead(wm offsetWatermark) {
	c.set(offsetCatalogueBlockHead, wm)
}

func (c *offsetCatalogue) Set(key string, wm offsetWatermark) {
	c.set(key, wm)
}

// Set stores or updates the watermark for the given key.
func (c *offsetCatalogue) set(key string, wm offsetWatermark) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = wm
	c.dirty = true
}

// tsdbCompactor wraps a tsdb.Compactor to record the Kafka offset watermark for each newly compacted block
// in the offset catalogue.
//
// Before each compaction tick the caller sets the watermark via SetOffset. New blocks produced during compaction
// are stamped with that watermark.
type tsdbCompactor struct {
	compactor tsdb.Compactor
	catalogue *offsetCatalogue

	partition int32
	offset    atomic.Int64
}

var _ tsdb.Compactor = (*tsdbCompactor)(nil)

func newTSDBCompactor(compactor tsdb.Compactor, catalogue *offsetCatalogue, partition int32) *tsdbCompactor {
	return &tsdbCompactor{
		compactor: compactor,
		catalogue: catalogue,
		partition: partition,
	}
}

// SetOffset sets the offset watermark that will be stamped on new blocks.
func (c *tsdbCompactor) SetOffset(offset int64) {
	c.offset.Store(offset)
}

func (c *tsdbCompactor) Plan(dir string) ([]string, error) {
	return c.compactor.Plan(dir)
}

func (c *tsdbCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, base *tsdb.BlockMeta) ([]ulid.ULID, error) {
	ulids, err := c.compactor.Write(dest, b, mint, maxt, base)
	if err != nil {
		return ulids, err
	}
	c.updateCatalogue(ulids)
	return ulids, nil
}

func (c *tsdbCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	ulids, err := c.compactor.Compact(dest, dirs, open)
	if err != nil {
		return ulids, err
	}
	c.updateCatalogue(ulids)
	return ulids, nil
}

func (c *tsdbCompactor) CompactOOO(dest string, oooHead *tsdb.OOOCompactionHead) ([]ulid.ULID, error) {
	ulids, err := c.compactor.CompactOOO(dest, oooHead)
	if err != nil {
		return ulids, err
	}
	c.updateCatalogue(ulids)
	return ulids, nil
}

func (c *tsdbCompactor) updateCatalogue(ulids []ulid.ULID) {
	if len(ulids) == 0 {
		return
	}
	wm := offsetWatermark{
		Partition: c.partition,
		Offset:    c.offset.Load(),
	}
	for _, id := range ulids {
		c.catalogue.Set(id.String(), wm)
	}
}
