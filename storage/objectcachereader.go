package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

// PopulateCache loads massif and checkpoint data for the currently selected log into the cache.
// It initializes the Selected log cache if it does not exist, and populates the following fields:
//   - MassifPaths: maps massif indices to their storage paths (data and checkpoint).
//   - Starts: maps massif storage paths to their MassifStart metadata.
//   - MassifData: maps storage paths to their raw data ([]byte for massifs, nil for checkpoints).
//   - Checkpoints: maps checkpoint storage paths to their Checkpoint metadata.
//   - FirstMassifIndex, HeadMassifIndex: track the range of massif indices found.
//   - FirstSealIndex, HeadSealIndex: track the range of seal (checkpoint) indices found.
//
// The method returns an error if the log is not selected, if directory listing fails for reasons
// other than non-existence, or if reading any massif or checkpoint file fails.
func (s *CachingStore) PopulateCache(ctx context.Context) error {

	if s.SelectedLogID == nil {
		return storage.ErrLogNotSelected
	}
	var ok bool
	s.Selected, ok = s.Logs[string(s.SelectedLogID)]
	if !ok {
		s.Selected = &LogCache{
			MassifPaths:      make(map[uint32]*MassifStoragePaths),
			MassifData:       make(map[string][]byte),
			CheckpointData:   make(map[string][]byte),
			FirstMassifIndex: ^uint32(0),
			FirstSealIndex:   ^uint32(0),
		}
		s.Logs[string(s.SelectedLogID)] = s.Selected
	}

	var massifPaths []string
	var checkpointPaths []string

	if s.Opts.RootDir != "" {
		// Note: use "." for explicit activation of cwd

		// Note: the explicit provision of MassifFilename only serves to locate the directory
		massifsDir, err := s.PrefixPath(storage.ObjectMassifData)
		if err != nil {
			return fmt.Errorf("failed to get massif prefix for log %x: %w", s.SelectedLogID, err)
		}
		checkPointsDir, err := s.PrefixPath(storage.ObjectCheckpoint)
		if err != nil {
			return fmt.Errorf("failed to get checkpoint prefix for log %x: %w", s.SelectedLogID, err)
		}

		massifPaths, err = NewSuffixDirLister(s.Opts.MassifExtension).ListFiles(massifsDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to list massif files in %s: %w", massifsDir, err)
		}
		checkpointPaths, err = NewSuffixDirLister(s.Opts.SealExtension).ListFiles(checkPointsDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to list checkpoint files in %s: %w", checkPointsDir, err)
		}
	}

	if s.Opts.MassifFile != "" {
		massifPaths = append(massifPaths, s.Opts.MassifFile)
	}

	if s.Opts.CheckpointFile != "" {
		checkpointPaths = append(checkpointPaths, s.Opts.CheckpointFile)
	}

	for _, storagePath := range massifPaths {

		start, data, err := s.readStart(storagePath)
		if err != nil {
			return fmt.Errorf("failed to read massif start from %s: %w", storagePath, err)
		}
		s.Selected.MassifPaths[start.MassifIndex] = &MassifStoragePaths{
			Data: storagePath,
		}

		s.Selected.MassifData[storagePath] = data

		if start.MassifIndex < s.Selected.FirstMassifIndex {
			s.Selected.FirstMassifIndex = start.MassifIndex
		}
		if start.MassifIndex > s.Selected.HeadMassifIndex {
			s.Selected.HeadMassifIndex = start.MassifIndex
		}
	}
	for _, storagePath := range checkpointPaths {
		// Pre-populate the massif data map with empty data to indicate presence

		checkpt, data, err := s.readCheckpoint(storagePath)
		if err != nil {
			return fmt.Errorf("failed to read checkpoint from %s: %w", storagePath, err)
		}
		massifIndex := uint32(massifs.MassifIndexFromMMRIndex(s.Opts.StorageOptions.MassifHeight, checkpt.MMRState.MMRSize-1))

		s.Selected.CheckpointData[storagePath] = data

		// if we also have the massif path, keep the massif and checkpoint paths together
		var paths *MassifStoragePaths
		var ok bool
		if paths, ok = s.Selected.MassifPaths[massifIndex]; !ok {
			paths = &MassifStoragePaths{}
			s.Selected.MassifPaths[massifIndex] = paths
		}

		paths.Checkpoint = storagePath

		// update the range of known seal indices, which may be disjoint from the massif indices
		if massifIndex < s.Selected.FirstSealIndex {
			s.Selected.FirstSealIndex = massifIndex
		}
		if massifIndex > s.Selected.HeadSealIndex {
			s.Selected.HeadSealIndex = massifIndex
		}
	}
	return nil
}

// readStart reads and decodes the MassifStart header from the given storage path.
// It reads only the MassifStart header (not the full massif data), decodes it,
// and returns the MassifStart struct along with the raw header bytes.
// Returns an error if reading or decoding fails.
func (s *CachingStore) readStart(storagePath string) (*massifs.MassifStart, []byte, error) {
	data, err := s.readn(storagePath, massifs.StartHeaderSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read massif start from %s: %w", storagePath, err)
	}
	start, err := decodeStart(data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode massif start from %s: %w", storagePath, err)
	}

	return start, data, nil
}

// readCheckpoint reads and decodes a checkpoint file from the given storage path.
// It reads the entire checkpoint file, decodes the signed root, and returns the resulting Checkpoint struct.
// readCheckpoint processes the full checkpoint file to extract both the MMR state and the signed message.
func (s *CachingStore) readCheckpoint(storagePath string) (*massifs.Checkpoint, []byte, error) {
	data, err := s.read(storagePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read massif start from %s: %w", storagePath, err)
	}
	checkpt, err := decodeCheckpoint(*s.Opts.StorageOptions.CBORCodec, data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode checkpoint from %s: %w", storagePath, err)
	}
	return checkpt, data, nil
}
