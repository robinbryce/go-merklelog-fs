package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/mmr"
	"github.com/robinbryce/go-merklelog-fs/filecache"
)

type MassifCommitter struct {
	MassifFinder
}

func NewMassifCommitter(options *Options, opts ...massifs.Option) (*MassifCommitter, error) {
	c := &MassifCommitter{}
	if options == nil {
		options = &Options{}
	}
	for _, opt := range opts {
		opt(options)
		opt(&options.Options)
		opt(&options.Options.StorageOptions)
	}

	if err := c.Init(options); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *MassifCommitter) Init(opts *Options) error {
	if err := c.MassifFinder.Init(opts); err != nil {
		return err
	}
	return nil
}

func (c *MassifCommitter) GetAppendContext(
	ctx context.Context,
) (*massifs.MassifContext, error) {
	// There are 3 states to consider here
	// 1. No massifs exist -> setup context for creating first
	// 2. A previous full massif exists -> setup context for creating a new one
	// 3. The most recent massif is not full -> setup context for extending current massif

	massifIndex, err := c.HeadIndex(ctx, storage.ObjectMassifData)
	if errors.Is(err, storage.ErrLogEmpty) {
		return c.createFirstMassifContext()
	}
	// If we are creating, we need to read the bytes from the previous massif to
	// be able to make the first mmr entry from the root of the last massif.
	// So we always read the massif we find

	mc, err := c.GetMassifContext(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	// The current first & last is initialized from what we read

	// If the massif has space for more nodes, the context is ready and we have
	// all the state setup.  case 3: existing massif with space, !creating.
	//  This works because no matter which massif this is, just prior to
	// adding the last *leaf*, the occupied size will be less than the massif
	// base size. And adding the leaf and its necessary interior nodes will
	// immediately exceed or equal the base size configured for a massif.
	sz := massifs.TreeSize(mc.Start.MassifHeight)
	start := mc.LogStart()
	if uint64(len(mc.Data))-start < sz {
		return mc, nil
	}

	mcnew := *mc

	// if the previous is complete, attempt to start a new massif
	mcnew.Creating = true

	// re-create Start for the new blob
	err = mcnew.StartNextMassif()
	if err != nil {
		return nil, fmt.Errorf("failed to start next massif: %w", err)
	}

	return &mcnew, nil
}

func (c *MassifCommitter) CommitContext(ctx context.Context, mc *massifs.MassifContext) error {
	var err error
	var storagePath string

	// Check we have not over filled the massif.

	// Note that we need to account for the size based on the full range.  When
	// committing massifs after the first, additional nodes are always required to
	// "bury", the previous massif's nodes.

	// leaves that the height (not the height index) allows for.
	maxLeafIndex := ((mmr.HeightSize(uint64(mc.Start.MassifHeight))+1)>>1)*uint64(mc.Start.MassifIndex+1) - 1
	spurHeight := mmr.SpurHeightLeaf(maxLeafIndex)
	// The overall size of the massif that contains that many leaves.
	maxMMRSize := mmr.MMRIndex(maxLeafIndex) + spurHeight + 1

	count := mc.Count()

	// The last legal index is first leaf + count - 1. The last leaf index + the
	// height is the last node index + 1.  So we just don't subtract the one on
	// either clause.
	if mc.Start.FirstIndex+count > maxMMRSize {
		return massifs.ErrMassifFull
	}

	// TODO: CRITICAL: This filesystem implementation does not support optimistic
	// concurrency, if there are multiple processes extending the massif files,
	// they will get corrupted.
	// If we assume all writers are appending, which they should be, we can use
	// modification time in combination with file size at time of read for a
	// usefully robust heuristic.

	storagePath, err = c.Opts.PathProvider.GetStoragePath(mc.Start.MassifIndex, storage.ObjectMassifData)
	if err != nil {
		return fmt.Errorf("failed to get storage path for massif %d: %w", mc.Start.MassifIndex, err)
	}
	var massifFile io.WriteCloser

	// Also CRITICAL: We must set the not-exists option if we are creating a new
	// massif. so we don't racily overwrite a new massif
	if mc.Creating {
		// We can apply this condition with regular posix file systems.
		massifFile, err = filecache.OpenExclusiveCreate(storagePath)
		if err != nil {
			return fmt.Errorf("failed to create new massif file %s: %w", storagePath, err)
		}
	} else {
		// We could optimize here by appending, replace all for now
		massifFile, err = filecache.OpenCreate(storagePath)
		if err != nil {
			return fmt.Errorf("failed to open existing massif file %s: %w", storagePath, err)
		}
	}

	err = filecache.WriteAndClose(massifFile, mc.Data)

	if err != nil {
		return err
	}

	if paths, ok := c.Cache.Selected.MassifPaths[mc.Start.MassifIndex]; ok {
		// If we have a path, we can update the massif paths
		paths.Data = storagePath
	} else {
		c.Cache.Selected.MassifPaths[mc.Start.MassifIndex] = &filecache.MassifStoragePaths{
			Data: storagePath,
		}
	}
	if mc.Start.MassifIndex > c.Cache.Selected.HeadMassifIndex {
		c.Cache.Selected.HeadMassifIndex = mc.Start.MassifIndex
	} else if mc.Start.MassifIndex < c.Cache.Selected.FirstMassifIndex {
		c.Cache.Selected.FirstMassifIndex = mc.Start.MassifIndex
	}
	c.Cache.Selected.MassifData[storagePath] = mc.Data
	c.Cache.Selected.Starts[storagePath] = &mc.Start

	mc.Creating = false

	return err
}

func (c *MassifCommitter) createFirstMassifContext() (*massifs.MassifContext, error) {
	// XXX: TODO: we _could_ just roll an id so that we never need to deal with
	// the zero case. for the first massif that is entirely benign.
	start := massifs.NewMassifStart(0, uint32(c.Opts.CommitmentEpoch), c.Opts.MassifHeight, 0, 0)

	// the zero values, or those explicitly set above are correct
	data, err := start.MarshalBinary()
	if err != nil {
		return nil, err
	}

	mc := &massifs.MassifContext{
		Creating: true,
		// epoch, massifIndex and firstIndex are zero and prev root is 32 bytes of zero
		Start: start,
	}
	// We pre-allocate and zero-fill the index, see the commentary in StartNextMassif
	mc.Data = append(data, mc.InitIndexData()...)

	return mc, nil
}
