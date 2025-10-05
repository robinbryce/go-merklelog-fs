package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
)

func (s *CachingStore) GetStorageOptions() massifs.StorageOptions {
	return s.Opts.StorageOptions
}

// HeadIndex finds the last object and returns it's index without reading the
// data.
func (s *CachingStore) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	c := s.Selected
	if c == nil {
		return 0, storage.ErrLogNotSelected
	}

	switch otype {
	case storage.ObjectMassifData, storage.ObjectMassifStart:
		return s.Selected.HeadMassifIndex, nil
	case storage.ObjectCheckpoint:
		return s.Selected.HeadSealIndex, nil
	default:
		return 0, fmt.Errorf("unsupported object type %v", otype)
	}
}

func (s *CachingStore) MassifData(massifIndex uint32) ([]byte, bool, error) {

	storagePath, ok, err := s.dataPath(massifIndex)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// The path is discovered (or not) by PopulateCache given the configured directories.
		return nil, false, storage.ErrDoesNotExist
	}
	data, ok := s.Selected.MassifData[storagePath]
	return data, ok, nil
}

func (s *CachingStore) CheckpointData(massifIndex uint32) ([]byte, bool, error) {

	storagePath, ok, err := s.checkpointPath(massifIndex)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// The path is discovered (or not) by PopulateCache given the configured directories.
		return nil, false, fmt.Errorf("%w: checkpoint for massif %d not found", storage.ErrDoesNotExist, massifIndex)
	}

	data, ok := s.Selected.CheckpointData[storagePath]
	return data, ok, nil
}

// MassifReadN un-conditionally reads up to n bytes of the massif data The read
// data is both cached and returned. Subsequent calls to MassifData will return
// the cached data.
func (s *CachingStore) MassifReadN(ctx context.Context, massifIndex uint32, n int) ([]byte, error) {

	storagePath, ok, err := s.dataPath(massifIndex)
	if err != nil {
		return nil, err
	}
	if !ok {
		// The path is discovered (or not) by PopulateCache given the configured directories.
		return nil, storage.ErrDoesNotExist
	}

	var data []byte
	if n < 0 {
		data, err = s.read(storagePath)
	} else {
		data, err = s.readn(storagePath, n)
	}
	if err != nil {
		return nil, err
	}
	s.Selected.MassifData[storagePath] = data
	return data, nil
}

func (s *CachingStore) CheckpointRead(ctx context.Context, massifIndex uint32) ([]byte, error) {
	storagePath, ok, err := s.checkpointPath(massifIndex)
	if err != nil {
		return nil, err
	}
	if !ok {
		// The path is discovered (or not) by PopulateCache given the configured directories.
		return nil, fmt.Errorf("%w: checkpoint for massif %d not found", storage.ErrDoesNotExist, massifIndex)
	}

	data, err := s.read(storagePath)
	if err != nil {
		return nil, err
	}
	s.Selected.CheckpointData[storagePath] = data
	return data, nil
}

func (s *CachingStore) readn(filePath string, n int) ([]byte, error) {

	file, err := s.Opts.ReadOpener.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open file %s (%v)", storage.ErrDoesNotExist, filePath, err)
	}
	defer file.Close()

	data := make([]byte, n)
	_, err = file.Read(data)
	if err != nil || len(data) != n {
		if err == nil {
			return nil, fmt.Errorf("%w: failed to read %d bytes from file %s", storage.ErrDoesNotExist, n, filePath)
		}
		return nil, fmt.Errorf("%w: failed to read  file %s (%v)", storage.ErrDoesNotExist, filePath, err)
	}
	return data, nil
}

func (s *CachingStore) read(storagePath string) ([]byte, error) {

	file, err := s.Opts.ReadOpener.Open(storagePath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open file %s (%v)", storage.ErrDoesNotExist, storagePath, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read from %s: %w", storagePath, err)
	}
	return data, nil
}
