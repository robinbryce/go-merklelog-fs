package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/forestrie/go-merklelog/massifs"
	commoncbor "github.com/forestrie/go-merklelog/massifs/cbor"
	"github.com/forestrie/go-merklelog/massifs/storage"
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

	if s.Opts.CreateRootDir && s.Opts.RootDir != "" {
		if err := os.MkdirAll(s.Opts.RootDir, s.Opts.DirCreateMode); err != nil {
			return fmt.Errorf("failed to create root dir %s: %w", s.Opts.RootDir, err)
		}
	}

	if s.Opts.LogID != nil {
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

	if s.Opts.RootDir != "" {
		if !s.Opts.CreateRootDir {
			if stat, err := os.Stat(s.Opts.RootDir); err != nil || !stat.IsDir() {
				return fmt.Errorf("root dir %s is not a directory or cannot be accessed: %w", s.Opts.RootDir, err)
			}
		}
	}
	if s.Opts.MassifFile != "" {
		if stat, err := os.Stat(s.Opts.MassifFile); err != nil || stat.IsDir() {
			return fmt.Errorf("massif file %s is not a file or cannot be accessed: %w", s.Opts.MassifFile, err)
		}
	}
	if s.Opts.CheckpointFile != "" {
		if stat, err := os.Stat(s.Opts.CheckpointFile); err != nil || stat.IsDir() {
			return fmt.Errorf("checkpoint file %s is not a file or cannot be accessed: %w", s.Opts.CheckpointFile, err)
		}
	}

	// if c.Opts.Opener == nil {
	// 	return fmt.Errorf("Opener not set in options")
	// }

	return nil
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
