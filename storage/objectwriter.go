package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

func (s *CachingStore) HasCapability(feature storage.StorageFeature) bool {
	switch feature {
	case storage.OptimisticWrite:
		// assuming there is only a single writer per log
		return true
	default:
		return false
	}
}

func (s *CachingStore) Put(
	ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte,
	failIfExists bool,
) error {

	if s.Selected == nil {
		return storage.ErrLogNotSelected
	}

	var storagePath string
	var err error

	storagePath, err = s.Opts.PathProvider.GetStoragePath(massifIndex, ty)
	if err != nil {
		return fmt.Errorf("failed to get storage path for massif index %d, type %v: %w", massifIndex, ty, err)
	}
	dir := filepath.Dir(storagePath)
	if err := os.MkdirAll(dir, s.Opts.DirCreateMode); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	var f io.WriteCloser
	if failIfExists {
		f, err = s.Opts.WriteOpener.OpenCreate(storagePath)
	} else {
		f, err = s.Opts.WriteOpener.OpenWrite(storagePath)
	}
	if err != nil {
		return fmt.Errorf("failed to open storage path %s for writing: %w", storagePath, err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write data to storage path %s: %w", storagePath, err)
	}


	paths, ok := s.Selected.MassifPaths[massifIndex]
	if !ok {
		paths = &MassifStoragePaths{}
	}

	switch ty {
	case storage.ObjectMassifData, storage.ObjectMassifStart:
		s.Selected.MassifData[storagePath] = data

		// if this is a replace, invalidate and defer re-population of the start
		s.Selected.Starts[storagePath] = nil

		if massifIndex > s.Selected.HeadMassifIndex {
			s.Selected.HeadMassifIndex = massifIndex
		}
		if massifIndex < s.Selected.FirstMassifIndex {
			s.Selected.FirstMassifIndex = massifIndex
		}
		paths.Data = storagePath

	case storage.ObjectCheckpoint:

		s.Selected.CheckpointData[storagePath] = data
		// if this is a replace, invalidate and defer re-population
		s.Selected.Checkpoints[storagePath] = nil
		if massifIndex > s.Selected.HeadSealIndex {
			s.Selected.HeadSealIndex = massifIndex
		}
		if massifIndex < s.Selected.FirstSealIndex {
			s.Selected.FirstSealIndex = massifIndex
		}
		paths.Checkpoint = storagePath

	default:
		return fmt.Errorf("unsupported object type %v", ty)
	}
	s.Selected.MassifPaths[massifIndex] = paths

	return nil
}
