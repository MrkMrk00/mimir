// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetCatalogue_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	c1 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	c1.Set(offsetCatalogueBlockHead, offsetWatermark{Topic: "ingest", Partition: 3, Offset: 500})
	c1.Set(block1.String(), offsetWatermark{Topic: "ingest", Partition: 3, Offset: 400})
	c1.Set(block2.String(), offsetWatermark{Topic: "ingest", Partition: 3, Offset: 450})
	require.NoError(t, c1.Save())

	_, err := os.Stat(filepath.Join(dir, offsetCatalogueFilename))
	require.NoError(t, err)

	c2 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	require.NoError(t, c2.Load())

	got, ok := c2.Get(offsetCatalogueBlockHead)
	require.True(t, ok)
	assert.Equal(t, int64(500), got.Offset)

	got, ok = c2.Get(block1.String())
	require.True(t, ok)
	assert.Equal(t, int64(400), got.Offset)

	got, ok = c2.Get(block2.String())
	require.True(t, ok)
	assert.Equal(t, int64(450), got.Offset)
}

func TestOffsetCatalogue_Prune(t *testing.T) {
	dir := t.TempDir()
	block1 := ulid.MustNew(1, nil)
	stale := ulid.MustNew(2, nil)

	c := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	c.Set(offsetCatalogueBlockHead, offsetWatermark{Offset: 500})
	c.Set(block1.String(), offsetWatermark{Offset: 400})
	c.Set(stale.String(), offsetWatermark{Offset: 450})

	c.Prune([]ulid.ULID{block1})

	_, ok := c.Get(offsetCatalogueBlockHead)
	assert.True(t, ok)

	_, ok = c.Get(block1.String())
	assert.True(t, ok)

	_, ok = c.Get(stale.String())
	assert.False(t, ok)
}

func TestOffsetCatalogue_LoadToleratesMissingAndCorruptFiles(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		c := newOffsetCatalogue(log.NewNopLogger(), t.TempDir(), "tenant-1")
		require.NoError(t, c.Load())
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})

	t.Run("corrupt file", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, offsetCatalogueFilename), []byte("not json"), 0644))

		c := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
		require.NoError(t, c.Load())
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})

	t.Run("unknown version", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, offsetCatalogueFilename), []byte(`{"version":99,"data":{}}`), 0644))

		c := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
		require.NoError(t, c.Load())
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})
}

// mockCompactor implements tsdb.Compactor for testing tsdbCompactor.
type mockCompactor struct {
	planResult    []string
	writeResult   []ulid.ULID
	compactResult []ulid.ULID
	oooResult     []ulid.ULID
	err           error

	writeCalled   bool
	compactCalled bool
	oooCalled     bool
}

func (m *mockCompactor) Plan(string) ([]string, error) {
	return m.planResult, m.err
}

func (m *mockCompactor) Write(string, tsdb.BlockReader, int64, int64, *tsdb.BlockMeta) ([]ulid.ULID, error) {
	m.writeCalled = true
	return m.writeResult, m.err
}

func (m *mockCompactor) Compact(string, []string, []*tsdb.Block) ([]ulid.ULID, error) {
	m.compactCalled = true
	return m.compactResult, m.err
}

func (m *mockCompactor) CompactOOO(string, *tsdb.OOOCompactionHead) ([]ulid.ULID, error) {
	m.oooCalled = true
	return m.oooResult, m.err
}

func newTestTSDBCompactor(t *testing.T, inner *mockCompactor, wm offsetWatermark) (*tsdbCompactor, *offsetCatalogue) {
	t.Helper()
	cat := newOffsetCatalogue(log.NewNopLogger(), t.TempDir(), "tenant-1")
	c := newTSDBCompactor(inner, cat, wm)
	return c, cat
}

var testWatermark = offsetWatermark{Topic: "ingest", Partition: 0, Offset: 500}

func TestTSDBCompactor_RecordsBlocks(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	t.Run("Write", func(t *testing.T) {
		c, cat := newTestTSDBCompactor(t, &mockCompactor{writeResult: []ulid.ULID{block1}}, testWatermark)

		ulids, err := c.Write("dest", nil, 0, 100, nil)
		require.NoError(t, err)
		assert.Equal(t, []ulid.ULID{block1}, ulids)

		got, ok := cat.Get(block1.String())
		require.True(t, ok)
		assert.Equal(t, testWatermark, got)
	})

	t.Run("Compact", func(t *testing.T) {
		c, cat := newTestTSDBCompactor(t, &mockCompactor{compactResult: []ulid.ULID{block1, block2}}, testWatermark)

		ulids, err := c.Compact("dest", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, []ulid.ULID{block1, block2}, ulids)

		got, ok := cat.Get(block1.String())
		require.True(t, ok)
		assert.Equal(t, testWatermark, got)

		got, ok = cat.Get(block2.String())
		require.True(t, ok)
		assert.Equal(t, testWatermark, got)
	})

	t.Run("CompactOOO", func(t *testing.T) {
		c, cat := newTestTSDBCompactor(t, &mockCompactor{oooResult: []ulid.ULID{block1}}, testWatermark)

		ulids, err := c.CompactOOO("dest", nil)
		require.NoError(t, err)
		assert.Equal(t, []ulid.ULID{block1}, ulids)

		got, ok := cat.Get(block1.String())
		require.True(t, ok)
		assert.Equal(t, testWatermark, got)
	})

	t.Run("empty result does not record", func(t *testing.T) {
		c, cat := newTestTSDBCompactor(t, &mockCompactor{writeResult: nil}, testWatermark)

		ulids, err := c.Write("dest", nil, 0, 100, nil)
		require.NoError(t, err)
		assert.Empty(t, ulids)

		_, ok := cat.Get(block1.String())
		assert.False(t, ok)
	})

	t.Run("error does not record", func(t *testing.T) {
		c, cat := newTestTSDBCompactor(t, &mockCompactor{err: assert.AnError}, testWatermark)

		_, err := c.Write("dest", nil, 0, 100, nil)
		require.Error(t, err)

		_, ok := cat.Get(block1.String())
		assert.False(t, ok)
	})
}

func TestTSDBCompactor_SetWatermark(t *testing.T) {
	block := ulid.MustNew(1, nil)

	c, cat := newTestTSDBCompactor(t, &mockCompactor{writeResult: []ulid.ULID{block}}, testWatermark)

	updated := offsetWatermark{Topic: "ingest", Partition: 0, Offset: 800}
	c.SetOffset(updated)

	_, err := c.Write("dest", nil, 0, 100, nil)
	require.NoError(t, err)

	got, ok := cat.Get(block.String())
	require.True(t, ok)
	assert.Equal(t, updated, got)
}
