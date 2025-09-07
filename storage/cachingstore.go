package storage

import (
	"bytes"
	"context"
	"fmt"

	commoncbor "github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type CachingStore struct {
	Opts          Options
	SelectedLogID storage.LogID
	Logs          map[string]*LogCache
	Selected      *LogCache
}

func (s *CachingStore) Init(ctx context.Context, parent *Options, vopts ...massifs.Option) error {

	var err error
	s.Opts = parent.Clone()
	for _, opt := range vopts {
		opt(s.Opts)
		opt(&s.Opts.StorageOptions)
	}
	if err = s.Opts.FillDefaults(); err != nil {
		return err
	}

	s.Logs = make(map[string]*LogCache)

	err = s.checkOptions()
	if err != nil {
		return err
	}

	if s.Opts.LogID != nil && s.Opts.PathProvider != nil {
		if err := s.SelectLog(ctx, s.Opts.LogID); err != nil {
			return fmt.Errorf("failed to select log %s: %w", s.Opts.LogID, err)
		}
	}
	return nil
}

func (s *CachingStore) SelectLog(ctx context.Context, logId storage.LogID) error {
	if s.Logs == nil {
		return fmt.Errorf("massif cache not initialized")
	}
	if logId == nil {
		return fmt.Errorf("logId cannot be nil")
	}

	if bytes.Equal(logId, s.SelectedLogID) && s.Selected != nil {
		return nil // Already selected
	}

	s.SelectedLogID = logId

	return s.PopulateCache(ctx)
}

func (s *CachingStore) checkOptions() error {
	if s.Opts.StorageOptions.CBORCodec == nil {
		return fmt.Errorf("missing CBORCodec in options")
	}
	if s.Opts.StorageOptions.MassifHeight == 0 {
		return fmt.Errorf("missing MassifHeight in options")
	}
	if s.Opts.ReadOpener == nil {
		return fmt.Errorf("a ReadOpener must be provided")
	}

	// if c.Opts.Opener == nil {
	// 	return fmt.Errorf("Opener not set in options")
	// }

	return nil
}

// start returns the cached start record if it is present in the cache.
// a false return indicates it is not present.
func (s *CachingStore) start(massifIndex uint32) (*massifs.MassifStart, bool, error) {

	var err error
	var start *massifs.MassifStart
	var path string
	var ok bool
	var data []byte
	path, ok, err = s.dataPath(massifIndex)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	if start, ok = s.Selected.Starts[path]; ok {
		return start, true, nil
	}

	// Put can invalidate the cached start, and we lazily re-populate it
	if data, ok = s.Selected.MassifData[path]; !ok {
		return nil, false, nil
	}
	start, err = decodeStart(data)
	if err != nil {
		return nil, false, err
	}

	s.Selected.Starts[path] = start

	return start, true, nil
}

func (s *CachingStore) checkpoint(massifIndex uint32) (*massifs.Checkpoint, bool, error) {

	var err error
	var checkpt *massifs.Checkpoint
	var path string
	var ok bool
	var data []byte
	path, ok, err = s.checkpointPath(massifIndex)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	if checkpt, ok = s.Selected.Checkpoints[path]; ok {
		return checkpt, true, nil
	}
	data, ok = s.Selected.CheckpointData[path]
	if !ok {
		return nil, false, nil
	}

	checkpt, err = decodeCheckpoint(*s.Opts.StorageOptions.CBORCodec, data)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode checkpoint from cached data: %w", err)
	}
	return checkpt, true, nil
}

func (s *CachingStore) paths(massifIndex uint32) (*MassifStoragePaths, bool, error) {
	if s.Selected == nil {
		return nil, false, storage.ErrLogNotSelected
	}

	paths, ok := s.Selected.MassifPaths[massifIndex]
	if !ok {
		return nil, false, nil
	}
	return paths, true, nil
}

func (s *CachingStore) dataPath(massifIndex uint32) (string, bool, error) {

	paths, ok, err := s.paths(massifIndex)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	if paths.Data == "" {
		// this is an error, if either the massif data or start was discovered, the data path should be set
		return "", false, fmt.Errorf("data path unknown for massif index %d", massifIndex)
	}
	return paths.Data, ok, nil
}

func (s *CachingStore) checkpointPath(massifIndex uint32) (string, bool, error) {

	paths, ok, err := s.paths(massifIndex)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	if paths.Checkpoint == "" {
		// this is an error, if either the massif data or start was discovered, the data path should be set
		return "", false, fmt.Errorf("checkpoint path unknown for massif index %d", massifIndex)
	}
	return paths.Checkpoint, ok, nil
}

func decodeStart(data []byte) (*massifs.MassifStart, error) {
	if len(data) < massifs.StartHeaderSize {
		return nil, fmt.Errorf("data too small to contain a valid start header")
	}

	ms := &massifs.MassifStart{}
	err := massifs.DecodeMassifStart(ms, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode massif start: %w", err)
	}
	return ms, nil
}

func decodeCheckpoint(codec commoncbor.CBORCodec, data []byte) (*massifs.Checkpoint, error) {
	cachedMessage, unverifiedState, err := massifs.DecodeSignedRoot(codec, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode signed root: %w", err)
	}
	return &massifs.Checkpoint{
		MMRState:     unverifiedState,
		Sign1Message: *cachedMessage,
	}, nil
}
