package storage

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

// Start returns the cached start record if it is present in the cache.
func (s *CachingStore) Start(massifIndex uint32) (*massifs.MassifStart, bool, error) {
	start, ok, err := s.start(massifIndex)
	return start, ok, err
}

func (s *CachingStore) SetStart(massifIndex uint32, start *massifs.MassifStart) error {
	if s.Selected == nil {
		return storage.ErrLogNotSelected
	}

	// Assuming the start was read through the cache, then it must have a path or it is an error
	var ok bool
	var paths *MassifStoragePaths
	if paths, ok = s.Selected.MassifPaths[massifIndex]; !ok || paths.Data == "" {
		return fmt.Errorf("no massif data path known for massif index %d", massifIndex)
	}
	s.Selected.Starts[paths.Data] = start
	return nil
}

func (s *CachingStore) Checkpoint(massifIndex uint32) (*massifs.Checkpoint, bool, error) {
	checkpt, ok, err := s.checkpoint(massifIndex)
	return checkpt, ok, err
}

func (s *CachingStore) SetCheckpoint(massifIndex uint32, checkpt *massifs.Checkpoint) error {
	if s.Selected == nil {
		return storage.ErrLogNotSelected
	}

	// Assuming the checkpoint was read through the cache, then it must have a path or it is an error
	var ok bool
	var paths *MassifStoragePaths
	if paths, ok = s.Selected.MassifPaths[massifIndex]; !ok || paths.Checkpoint == "" {
		return fmt.Errorf("no checkpoint path known for massif index %d", massifIndex)
	}
	s.Selected.Checkpoints[paths.Checkpoint] = checkpt
	return nil
}
