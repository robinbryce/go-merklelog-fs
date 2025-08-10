package filecache

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type MassifStoragePaths struct {
	Data       string
	Checkpoint string
}

type LogCache struct {
	MassifPaths      map[uint32]*MassifStoragePaths
	Starts           map[string]*massifs.MassifStart
	MassifData       map[string][]byte
	Checkpoints      map[string]*massifs.Checkpoint
	FirstMassifIndex uint32
	HeadMassifIndex  uint32
	FirstSealIndex   uint32
	HeadSealIndex    uint32
}

type Cache struct {
	Opts          Options
	SelectedLogID storage.LogID
	Logs          map[string]*LogCache
	Selected      *LogCache
}

func NewCache(options Options, opts ...massifs.Option) (*Cache, error) {

	cache := &Cache{
		Opts: options,
	}
	for _, opt := range opts {
		opt(cache.Opts)
	}
	if err := cache.checkOptions(); err != nil {
		return nil, err
	}
	cache.Logs = make(map[string]*LogCache)
	return cache, nil
}

// Interface methods

func (c *Cache) SelectLog(logId storage.LogID) error {

	if bytes.Equal(logId, c.SelectedLogID) && c.Selected != nil {
		return nil // Already selected
	}

	var ok bool
	c.SelectedLogID = logId
	c.Selected, ok = c.Logs[string(logId)]
	if !ok {
		c.Selected = &LogCache{
			MassifPaths:      make(map[uint32]*MassifStoragePaths),
			Starts:           make(map[string]*massifs.MassifStart),
			MassifData:       make(map[string][]byte),
			Checkpoints:      make(map[string]*massifs.Checkpoint),
			FirstMassifIndex: ^uint32(0),
			FirstSealIndex:   ^uint32(0),
		}
		c.Logs[string(logId)] = c.Selected
	}

	return nil
}

func (c *Cache) Extents(ty storage.ObjectType) (uint32, uint32) {
	if c.Selected == nil {
		return 0, 0
	}
	switch ty {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		return c.Selected.FirstMassifIndex, c.Selected.HeadMassifIndex
	case storage.ObjectCheckpoint:
		return c.Selected.FirstSealIndex, c.Selected.HeadSealIndex
	default:
		return 0, 0
	}
}

func (c *Cache) HeadIndex(ty storage.ObjectType) uint32 {
	_, hi := c.Extents(ty)
	return hi

}

func (c *Cache) Prime(ctx context.Context, storagePath string, ty storage.ObjectType) error {
	if c.Selected == nil {
		return storage.ErrLogNotSelected
	}
	if ty != storage.ObjectMassifData {
		return c.Read(ctx, storagePath, ty)
	}

	// Prime by reading the start of the massif
	return c.Read(ctx, storagePath, storage.ObjectMassifStart)
}

func (c *Cache) Read(ctx context.Context, storagePath string, ty storage.ObjectType) error {

	var ok bool
	var paths *MassifStoragePaths

	if c.Selected == nil {
		return storage.ErrLogNotSelected
	}

	switch ty {
	case storage.ObjectMassifStart:
		start, err := c.readStart(storagePath)
		if err != nil {
			return fmt.Errorf("failed to read massif start: %w", err)
		}
		c.Selected.Starts[storagePath] = start
		if paths, ok = c.Selected.MassifPaths[start.MassifIndex]; !ok {
			paths = &MassifStoragePaths{}
			c.Selected.MassifPaths[start.MassifIndex] = paths
		}
		// the start is read from the massif file so its path is the same as the massif data path
		paths.Data = storagePath

		// the massif data may have been read independently of the start. so we don't tuck this away in the new case above
		if start.MassifIndex < c.Selected.FirstMassifIndex {
			c.Selected.FirstMassifIndex = start.MassifIndex
		}
		if start.MassifIndex > c.Selected.HeadMassifIndex {
			c.Selected.HeadMassifIndex = start.MassifIndex
		}

	case storage.ObjectMassifData:
		// Handle reading massif data
		data, start, err := c.readData(storagePath)
		if err != nil {
			return fmt.Errorf("failed to read massif data: %w", err)
		}
		c.Selected.MassifData[storagePath] = data
		c.Selected.Starts[storagePath] = start
		c.Selected.Starts[storagePath] = start

		if paths, ok = c.Selected.MassifPaths[start.MassifIndex]; !ok {
			paths = &MassifStoragePaths{}
			c.Selected.MassifPaths[start.MassifIndex] = paths
		}
		paths.Data = storagePath
		// the start data may have been read independently of the start. so we don't tuck this away in the new case above
		if start.MassifIndex < c.Selected.FirstMassifIndex {
			c.Selected.FirstMassifIndex = start.MassifIndex
		}
		if start.MassifIndex > c.Selected.HeadMassifIndex {
			c.Selected.HeadMassifIndex = start.MassifIndex
		}

	case storage.ObjectCheckpoint:
		// Handle reading checkpoint data
		checkpoint, err := c.readCheckpoint(storagePath)
		if err != nil {
			return fmt.Errorf("failed to read checkpoint: %w", err)
		}
		c.Selected.Checkpoints[storagePath] = checkpoint
		massifIndex := uint32(massifs.MassifIndexFromMMRIndex(c.Opts.StorageOptions.MassifHeight, checkpoint.MMRState.MMRSize-1))

		if paths, ok = c.Selected.MassifPaths[massifIndex]; !ok {
			paths = &MassifStoragePaths{}
			c.Selected.MassifPaths[massifIndex] = paths
		}
		paths.Checkpoint = storagePath
		if massifIndex < c.Selected.FirstMassifIndex {
			c.Selected.FirstSealIndex = massifIndex
		}
		if massifIndex > c.Selected.HeadMassifIndex {
			c.Selected.HeadSealIndex = massifIndex
		}

	default:
		return fmt.Errorf("unsupported object type: %v", ty)
	}

	return nil
}

func (c *Cache) GetStart(ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error) {
	_, start, err := c.getStart(massifIndex)
	if err != nil {
		return nil, err
	}
	return start, nil
}

func (c *Cache) GetData(ctx context.Context, massifIndex uint32) ([]byte, error) {
	data, _, err := c.getData(massifIndex)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Cache) Get(ctx context.Context, massifIndex uint32) ([]byte, *massifs.MassifStart, *massifs.Checkpoint, error) {

	var ok bool
	var data []byte
	var start *massifs.MassifStart
	var checkpt *massifs.Checkpoint

	paths, start, err := c.getStart(massifIndex)
	if err != nil {
		return nil, nil, nil, err
	}

	data, ok = c.Selected.MassifData[paths.Data]
	if !ok {
		data, start, err = c.readData(paths.Data)
		if err != nil {
			return nil, nil, nil, err
		}
		// Note: the data from the massif file may legitimately be fresher than
		// the cached start there is no current way to identify the log from its
		// start header.
		c.Selected.MassifData[paths.Data] = data
		c.Selected.Starts[paths.Data] = start
	}

	if paths.Checkpoint == "" {
		return data, start, nil, nil
	}

	checkpt, ok = c.Selected.Checkpoints[paths.Checkpoint]
	if !ok {
		checkpt, err = c.readCheckpoint(paths.Checkpoint)
		if err != nil {
			return nil, nil, nil, err
		}
		c.Selected.Checkpoints[paths.Checkpoint] = checkpt
	}
	return data, start, checkpt, nil
}

// Private methods

func (c *Cache) getData(massifIndex uint32) ([]byte, *massifs.MassifStart, error) {
	var ok bool
	var data []byte
	var start *massifs.MassifStart

	paths, start, err := c.getStart(massifIndex)
	if err != nil {
		return nil, nil, err
	}

	data, ok = c.Selected.MassifData[paths.Data]
	if !ok {
		data, start, err = c.readData(paths.Data)
		if err != nil {
			return nil, nil, err
		}
		// Note: the data from the massif file may legitimately be fresher than
		// the cached start there is no current way to identify the log from its
		// start header.
		c.Selected.MassifData[paths.Data] = data
		c.Selected.Starts[paths.Data] = start
	}
	return data, start, nil
}

func (c *Cache) getStart(massifIndex uint32) (*MassifStoragePaths, *massifs.MassifStart, error) {
	if c.Selected == nil {
		return nil, nil, storage.ErrLogNotSelected
	}

	if paths, ok := c.Selected.MassifPaths[massifIndex]; ok {
		if paths.Data == "" {
			return nil, nil, fmt.Errorf("%w: massif data path for index %d", storage.ErrNotAvailable, massifIndex)
		}
		if start, ok := c.Selected.Starts[paths.Data]; ok {
			return paths, start, nil
		}
	}
	return nil, nil, fmt.Errorf(
		"%w: massif start not found for index %d", storage.ErrNotAvailable, massifIndex)
}

func (c *Cache) readStart(storagePath string) (*massifs.MassifStart, error) {

	file, err := c.Opts.Opener.Open(storagePath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open file %s (%v)", storage.ErrDoesNotExist, storagePath, err)
	}
	defer file.Close()

	header := make([]byte, 32)
	n, err := file.Read(header)
	if err != nil || n != 32 {
		return nil, fmt.Errorf("failed to read massif start header from %s: %w", storagePath, err)
	}

	ms := massifs.MassifStart{}
	err = massifs.DecodeMassifStart(&ms, header)
	if err != nil {
		return nil, fmt.Errorf("failed to decode massif start: %w", err)
	}

	return &ms, nil
}

func (c *Cache) readData(storagePath string) ([]byte, *massifs.MassifStart, error) {
	if c.Opts.Opener == nil {
		return nil, nil, fmt.Errorf("Opener not set in options")
	}

	file, err := c.Opts.Opener.Open(storagePath)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: failed to open file %s (%v)", storage.ErrDoesNotExist, storagePath, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read data from %s: %w", storagePath, err)
	}
	ms := massifs.MassifStart{}
	err = massifs.DecodeMassifStart(&ms, data[:32])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode massif start from data: %w", err)
	}

	return data, &ms, nil
}

func (c *Cache) readCheckpoint(storagePath string) (*massifs.Checkpoint, error) {
	if c.Opts.Opener == nil {
		return nil, fmt.Errorf("Opener not set in options")
	}

	file, err := c.Opts.Opener.Open(storagePath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open file %s (%v)", storage.ErrDoesNotExist, storagePath, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint data from %s: %w", storagePath, err)
	}

	cachedMessage, unverifiedState, err := massifs.DecodeSignedRoot(*c.Opts.StorageOptions.CBORCodec, data)
	if err != nil {
		return nil, err
	}

	return &massifs.Checkpoint{
		MMRState:     unverifiedState,
		Sign1Message: *cachedMessage,
	}, nil
}

func (c *Cache) checkOptions() error {
	if c.Opts.StorageOptions.CBORCodec == nil {
		return fmt.Errorf("missing CBORCodec in options")
	}
	if c.Opts.StorageOptions.MassifHeight == 0 {
		return fmt.Errorf("missing MassifHeight in options")
	}

	if c.Opts.Opener == nil {
		return fmt.Errorf("Opener not set in options")
	}

	return nil
}
