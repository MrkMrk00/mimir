// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	offsetCatalogueVersion = 1
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

type offsetReader interface {
	LastSeenOffset() int64
}

type offsetCatalogue struct {
	logger log.Logger
	dir    string
	userID string

	partition int32

	offsetReader offsetReader

	mu   sync.Mutex
	data map[string]offsetWatermark
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

func (c *offsetCatalogue) Sync(ctx context.Context) error {
	spanLogger, ctx := spanlogger.New(ctx, c.logger, tracer, "Ingester.OffsetCatalogue.Sync")
	defer spanLogger.Finish()

	lastSeenOffset := c.offsetReader.LastSeenOffset()

	oldData, err := readOffsetCatalogueFromFile(c.dir)
	if err != nil {
		return fmt.Errorf("read offset catalogue: %w", err)
	}

	blocks := make(map[string]struct{})
	for id, err := range listBlocks(c.dir) {
		if err != nil {
			return err
		}
		blocks[id.String()] = struct{}{}
	}

	c.mu.Lock()
	catalogueData := c.data
	clear(c.data)
	c.mu.Unlock()

	data := offsetCatalogueData{
		Version: offsetCatalogueVersion,
		Data:    make(map[string]offsetWatermark, len(blocks)),
	}
	for id := range blocks {
		if wmk, ok := oldData.Data[id]; ok {
			// If block already exists in the previous catalogue, keep it.
			data.Data[id] = wmk
			continue
		}
		wmk, ok := catalogueData[id]
		if !ok || wmk.Offset < 0 {
			// If block wasn't found in the catalogue (e.g. block existed before start),
			// or block's watermark offset wasn't captured, fallback to the most recent lastSeenOffset.
			// This is conservative: if block was found on disk, its data came from offset lower than current lastSeenOffset.
			wmk = offsetWatermark{
				Partition: c.partition,
				Offset:    lastSeenOffset,
			}
		} else if wmk.Offset > lastSeenOffset {
			level.Warn(spanLogger).Log("msg", "found unexpected offset watermark", "user", c.userID, "block", id, "partition", wmk.Partition, "offset", wmk.Offset, "last_seen_offset", lastSeenOffset)
			continue
		}
		data.Data[id] = wmk
	}

	if err := writeOffsetCatalogueToFile(c.logger, c.dir, data); err != nil {
		level.Warn(spanLogger).Log("msg", "writing offset catalogue failed", "user", c.userID, "err", err)
	}

	return nil
}

func readOffsetCatalogueFromFile(dir string) (offsetCatalogueData, error) {
	filePath := filepath.Join(dir, offsetCatalogueFilename)
	b, err := os.ReadFile(filePath)
	if err != nil {
		return offsetCatalogueData{}, fmt.Errorf("read %s: %w", filePath, err)
	}

	var data offsetCatalogueData
	if err := json.Unmarshal(b, &data); err != nil {
		return offsetCatalogueData{}, fmt.Errorf("parse json %s: %w", filePath, err)
	}
	if data.Version != offsetCatalogueVersion {
		return offsetCatalogueData{}, fmt.Errorf("expected version %d got %d", offsetCatalogueVersion, data.Version)
	}
	return data, nil
}

func writeOffsetCatalogueToFile(logger log.Logger, dir string, data offsetCatalogueData) (err error) {
	filePath := filepath.Join(dir, offsetCatalogueFilename)

	f, err := atomicfs.Create(filePath)
	if err != nil {
		return fmt.Errorf("create %s: %w", filePath, err)
	}
	defer runutil.CloseWithErrCapture(&err, f, "write offset catalogue to file %s", filePath)

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(data); err != nil {
		return err
	}
	return nil
}

func (c *offsetCatalogue) SetOffset(key string, offset int64) {
	wmk := offsetWatermark{
		Partition: c.partition,
		Offset:    offset,
	}
	c.mu.Lock()
	c.data[key] = wmk
	c.mu.Unlock()
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
