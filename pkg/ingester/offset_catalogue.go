// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
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

//type offsetCatalogueData struct {
//	Version int                        `json:"version"`
//	Data    map[string]offsetWatermark `json:"data"`
//}

type offsetCatalogue struct {
	logger log.Logger
	dir    string
	userID string

	partition int32

	offsetReader offsetReader

	data sync.Map // ulid -> offset watermark
}

func newOffsetCatalogue(logger log.Logger, dir, userID string, partition int32, offsetReader offsetReader) *offsetCatalogue {
	return &offsetCatalogue{
		logger: logger,
		dir:    dir,
		userID: userID,

		partition:    partition,
		offsetReader: offsetReader,
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

//func (c *offsetCatalogue) GetHead() (offsetWatermark, bool) {
//	c.mu.Lock()
//	defer c.mu.Unlock()
//
//	wm, ok := c.data[offsetCatalogueBlockHead]
//	return wm, ok
//}
//
//func (c *offsetCatalogue) SetHead(wm offsetWatermark) {
//	c.set(offsetCatalogueBlockHead, wm)
//}

func (c *offsetCatalogue) SetOffset(key string, offset int64) {
	wm := offsetWatermark{
		Partition: c.partition,
		Offset:    offset,
	}
	c.data.Store(key, wm)
}

func tsdbCompactorFactory(db *userTSDB, catalogue *offsetCatalogue) tsdb.NewCompactorFunc {
	return func(ctx context.Context, r prometheus.Registerer, l *slog.Logger, ranges []int64, pool chunkenc.Pool, opts *tsdb.Options) (tsdb.Compactor, error) {
		compactor, err := tsdb.NewLeveledCompactorWithOptions(ctx, r, l, ranges, pool, tsdb.LeveledCompactorOptions{
			MaxBlockChunkSegmentSize:    opts.MaxBlockChunkSegmentSize,
			EnableOverlappingCompaction: opts.EnableOverlappingCompaction,
			PD:                          opts.PostingsDecoderFactory,
			UseUncachedIO:               opts.UseUncachedIO,
			BlockExcludeFilter:          opts.BlockCompactionExcludeFunc,
		})
		if err != nil {
			return nil, err
		}

		tsdbCompactor := newTSDBCompactor(compactor, catalogue)
		db.tsdbCompactor = tsdbCompactor

		return tsdbCompactor, nil
	}
}

type offsetReader interface {
	LastSeenOffset() int64
}

// tsdbCompactor wraps a tsdb.Compactor to record the Kafka offset watermark for each newly compacted block
// in the offset catalogue.
type tsdbCompactor struct {
	compactor tsdb.Compactor
	catalogue *offsetCatalogue
}

var _ tsdb.Compactor = (*tsdbCompactor)(nil)

func newTSDBCompactor(compactor tsdb.Compactor, catalogue *offsetCatalogue) *tsdbCompactor {
	return &tsdbCompactor{
		compactor: compactor,
		catalogue: catalogue,
	}
}

func (c *tsdbCompactor) Plan(dir string) ([]string, error) {
	return c.compactor.Plan(dir)
}

func (c *tsdbCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, base *tsdb.BlockMeta) ([]ulid.ULID, error) {
	return c.compactAndUpdateCatalogue(func() ([]ulid.ULID, error) {
		return c.compactor.Write(dest, b, mint, maxt, base)
	})
}

func (c *tsdbCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	return c.compactAndUpdateCatalogue(func() ([]ulid.ULID, error) {
		return c.compactor.Compact(dest, dirs, open)
	})
}

func (c *tsdbCompactor) CompactOOO(dest string, oooHead *tsdb.OOOCompactionHead) ([]ulid.ULID, error) {
	return c.compactAndUpdateCatalogue(func() ([]ulid.ULID, error) {
		return c.compactor.CompactOOO(dest, oooHead)
	})
}

func (c *tsdbCompactor) compactAndUpdateCatalogue(compactFunc func() ([]ulid.ULID, error)) ([]ulid.ULID, error) {
	// Record the last seen offset before running the compaction func. The "seen" records are already in the head by this time.
	offset := c.catalogue.offsetReader.LastSeenOffset()
	ulids, err := compactFunc()
	if err != nil {
		return ulids, err
	}
	for _, id := range ulids {
		c.catalogue.SetOffset(id.String(), offset)
	}
	return ulids, nil
}
