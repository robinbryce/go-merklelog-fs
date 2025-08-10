package filecache

import (
	"context"
	"fmt"
	"os"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
)

// Put writes the data without regard to whether the object already exists or any consideration of consistency.
func (c *Cache) Put(ctx context.Context, massifIndex uint32, data []byte, otype storage.ObjectType) error {
	if c.Selected == nil {
		return storage.ErrLogNotSelected
	}
	ms := massifs.MassifStart{}
	err := massifs.DecodeMassifStart(&ms, data[:32])
	if err != nil {
		return fmt.Errorf("failed to decode massif start: %w", err)
	}

	paths, ok := c.Selected.MassifPaths[massifIndex]
	if !ok {

		paths = &MassifStoragePaths{}

		prefix, err := c.Opts.PrefixProvider.Prefix(c.SelectedLogID, storage.ObjectMassifData)
		if err != nil {
			return fmt.Errorf("failed to get massif data prefix: %w", err)
		}
		if err = os.MkdirAll(prefix, 0755); err != nil {
			return fmt.Errorf("failed to create massif data directory %s: %w", prefix, err)
		}
		paths.Data = storageschema.FmtMassifPath(prefix, massifIndex)

	}
	// We are always provided the full massif data, not a delta. So the open
	// mode is O_TRUNC (empty the file if it exists).  The caller is responsible
	// for checking the local is consistent with the remote before replacing.

	dataFile, err := OpenCreate(paths.Data)
	if err != nil {
		return fmt.Errorf("failed to open/create massif data file %s: %w", paths.Data, err)
	}

	// Write the data first, because we could use the checkpoint to verify a roll back
	err = WriteAndClose(dataFile, data)
	if err != nil {
		// WriteAndClose will close the file even on error
		return fmt.Errorf("failed to write massif data to %s: %w", paths.Data, err)
	}

	// Replace the verified context in the cache
	c.Selected.MassifPaths[massifIndex] = paths // if they didn't exist, they are created now
	c.Selected.MassifData[paths.Data] = data
	c.Selected.Starts[paths.Data] = &ms

	return nil
}

func (c *Cache) ReplaceVerified(vc *massifs.VerifiedContext) error {

	var err error

	if c.Selected == nil {
		return storage.ErrLogNotSelected
	}

	paths, ok := c.Selected.MassifPaths[vc.Start.MassifIndex]
	if !ok {

		paths = &MassifStoragePaths{}

		prefix, err := c.Opts.PrefixProvider.Prefix(c.SelectedLogID, storage.ObjectMassifData)
		if err != nil {
			return fmt.Errorf("failed to get massif data prefix: %w", err)
		}
		if err = os.MkdirAll(prefix, 0755); err != nil {
			return fmt.Errorf("failed to create massif data directory %s: %w", prefix, err)
		}
		paths.Data = storageschema.FmtMassifPath(prefix, vc.Start.MassifIndex)

		prefix, err = c.Opts.PrefixProvider.Prefix(c.SelectedLogID, storage.ObjectCheckpoint)
		if err != nil {
			return fmt.Errorf("failed to get checkpoint prefix: %w", err)
		}

		if err = os.MkdirAll(prefix, 0755); err != nil {
			return fmt.Errorf("failed to create massif checkpoint directory %s: %w", prefix, err)
		}

		paths.Checkpoint = storageschema.FmtCheckpointPath(prefix, vc.Start.MassifIndex)
		// defer updating MassifPaths until we have successfully written the files
	}

	// We are always provided the full massif data, not a delta. So the open
	// mode is O_TRUNC (empty the file if it exists).  The caller is responsible
	// for checking the local is consistent with the remote before replacing.

	dataFile, err := OpenCreate(paths.Data)
	if err != nil {
		return fmt.Errorf("failed to open/create massif data file %s: %w", paths.Data, err)
	}
	// open both files before writing to avoid partial writes due to file path errors
	checkPtFile, err := OpenCreate(paths.Checkpoint)
	if err != nil {
		dataFile.Close()
		return fmt.Errorf("failed to open/create checkpoint file %s: %w", paths.Checkpoint, err)
	}

	// Write the data first, because we could use the checkpoint to verify a roll back
	err = WriteAndClose(dataFile, vc.Data)
	if err != nil {
		// WriteAndClose will close the file even on error
		return fmt.Errorf("failed to write massif data to %s: %w", paths.Data, err)
	}
	checkPtBytes, err := vc.Sign1Message.MarshalCBOR()
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint message: %w", err)
	}
	err = WriteAndClose(checkPtFile, checkPtBytes)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint to %s: %w", paths.Checkpoint, err)
	}

	// Replace the verified context in the cache
	c.Selected.MassifPaths[vc.Start.MassifIndex] = paths // if they didn't exist, they are created now
	c.Selected.MassifData[paths.Data] = vc.Data
	c.Selected.Starts[paths.Data] = &vc.Start
	c.Selected.Checkpoints[paths.Checkpoint] = &massifs.Checkpoint{}
	c.Selected.Checkpoints[paths.Checkpoint].MMRState = vc.MMRState
	c.Selected.Checkpoints[paths.Checkpoint].Sign1Message = vc.Sign1Message

	return nil
}
