// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const (
	offsetCatalogueVersion = 1

	// Special key that holds the watermark of the head block.
	offsetCatalogueBlockHead = "__head__"
)

type offsetWatermark struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
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

// Load reads the catalogue from disk. Entries for blocks not in existingBlocks
// are pruned (the __head__ entry is always kept). If the file does not exist or
// is corrupt the catalogue starts empty.
func (c *offsetCatalogue) Load(existingBlocks []ulid.ULID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.filePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read offset catalogue: %w", err)
	}

	var stored offsetCatalogueData
	if err := json.Unmarshal(data, &stored); err != nil {
		level.Warn(c.logger).Log("msg", "discarding corrupt offset catalogue", "path", c.filePath(), "err", err)
		return nil
	}
	if stored.Version != offsetCatalogueVersion {
		level.Warn(c.logger).Log("msg", "discarding offset catalogue with unknown version", "path", c.filePath(), "version", stored.Version)
		return nil
	}
	if stored.Data == nil {
		return nil
	}

	existing := make(map[string]struct{}, len(existingBlocks))
	for _, id := range existingBlocks {
		existing[id.String()] = struct{}{}
	}

	pruned := 0
	for key, wm := range stored.Data {
		if key == offsetCatalogueBlockHead {
			c.data[key] = wm
			continue
		}
		if _, ok := existing[key]; ok {
			c.data[key] = wm
		} else {
			pruned++
		}
	}

	if pruned > 0 {
		c.dirty = true
		level.Info(c.logger).Log("msg", "pruned stale entries from offset catalogue", "pruned", pruned, "remaining", len(c.data))
	}

	return nil
}

// Save persists the catalogue to disk atomically. No-op if nothing changed.
func (c *offsetCatalogue) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.dirty {
		return nil
	}

	stored := offsetCatalogueData{
		Version: offsetCatalogueVersion,
		Data:    c.data,
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	if err := atomicfs.CreateFile(c.filePath(), bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write offset catalogue: %w", err)
	}

	c.dirty = false
	return nil
}

// Get returns the watermark for the given key and whether it exists.
func (c *offsetCatalogue) Get(key string) (offsetWatermark, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	wm, ok := c.data[key]
	return wm, ok
}

// Set stores or updates the watermark for the given key.
func (c *offsetCatalogue) Set(key string, wm offsetWatermark) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = wm
	c.dirty = true
}

// offsetCompactor wraps a tsdb.Compactor to record the Kafka offset watermark
// for each newly compacted block in the offset catalogue.
type offsetCompactor struct {
	inner     tsdb.Compactor
	catalogue *offsetCatalogue
	watermark func() offsetWatermark
}

var _ tsdb.Compactor = (*offsetCompactor)(nil)

func newOffsetCompactor(inner tsdb.Compactor, catalogue *offsetCatalogue, watermark func() offsetWatermark) *offsetCompactor {
	return &offsetCompactor{
		inner:     inner,
		catalogue: catalogue,
		watermark: watermark,
	}
}

func (c *offsetCompactor) Plan(dir string) ([]string, error) {
	return c.inner.Plan(dir)
}

func (c *offsetCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, base *tsdb.BlockMeta) ([]ulid.ULID, error) {
	ulids, err := c.inner.Write(dest, b, mint, maxt, base)
	if err != nil {
		return ulids, err
	}
	c.recordBlocks(ulids)
	return ulids, nil
}

func (c *offsetCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	ulids, err := c.inner.Compact(dest, dirs, open)
	if err != nil {
		return ulids, err
	}
	c.recordBlocks(ulids)
	return ulids, nil
}

func (c *offsetCompactor) CompactOOO(dest string, oooHead *tsdb.OOOCompactionHead) ([]ulid.ULID, error) {
	ulids, err := c.inner.CompactOOO(dest, oooHead)
	if err != nil {
		return ulids, err
	}
	c.recordBlocks(ulids)
	return ulids, nil
}

func (c *offsetCompactor) recordBlocks(ulids []ulid.ULID) {
	if len(ulids) == 0 {
		return
	}
	wm := c.watermark()
	for _, id := range ulids {
		c.catalogue.Set(id.String(), wm)
	}
}
