package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	commoncbor "github.com/datatrails/go-datatrails-common/cbor"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
	"github.com/robinbryce/go-merklelog-fs/filecache"
	// "github.com/robinbryce/go-merklelog-fs/storage"
)

const (
	DefaultSealExt      = storageschema.V1MMRExtSep + storageschema.V1MMRSealSignedRootExt
	DefaultMassifExt    = storageschema.V1MMRExtSep + storageschema.V1MMRMassifExt
	DefaultMassifHeight = 14
)

var DefaultLogID = []byte{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

type Options struct {
	filecache.Options

	SealExtension   string // e.g. ".sth"
	MassifExtension string // e.g. ".log"
}

type CheckpointOptions struct{}

type MassifFinder struct {
	Opts  *Options
	Cache *filecache.Cache
}

func NewMassifFinder(options *Options, opts ...massifs.Option) (*MassifFinder, error) {
	var err error

	if options == nil {
		options = &Options{}
	}

	for _, opt := range opts {
		opt(options)
		opt(&options.Options)
	}

	f := &MassifFinder{}
	if err = f.Init(options); err != nil {
		return nil, fmt.Errorf("failed to init massif finder: %w", err)
	}
	return f, nil
}

func (f *MassifFinder) Init(opts *Options) error {

	var err error

	f.Opts = opts

	if f.Opts.MassifHeight == 0 {
		f.Opts.MassifHeight = DefaultMassifHeight
	}

	if f.Opts.CBORCodec == nil {
		var codec commoncbor.CBORCodec
		if codec, err = massifs.NewCBORCodec(); err != nil {
			return err
		}
		f.Opts.CBORCodec = &codec
	}

	if f.Opts.SealExtension == "" {
		f.Opts.SealExtension = DefaultSealExt
	}

	if f.Opts.PrefixProvider == nil {
		return fmt.Errorf("a prefix provider is required")
	}

	if f.Opts.MassifExtension == "" {
		f.Opts.MassifExtension = DefaultMassifExt
	}
	if f.Opts.Opener == nil {
		f.Opts.Opener = NewFileOpener()
	}

	f.Cache, err = filecache.NewCache(f.Opts.Options)
	if err != nil {
		return fmt.Errorf("failed to create massif cache: %w", err)
	}

	return nil
}

func NewMassifContext(data []byte) (*massifs.MassifContext, error) {
	var err error
	mc := massifs.MassifContext{
		MassifData: massifs.MassifData{
			Data: data,
		},
	}
	err = massifs.DecodeMassifStart(&mc.Start, data[:32])
	if err != nil {
		return nil, fmt.Errorf("failed to decode massif start: %w", err)
	}

	if err = mc.CreatePeakStackMap(); err != nil {
		return nil, fmt.Errorf("failed to create peak stack map: %w", err)
	}
	return &mc, nil
}

func (f MassifFinder) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	if f.Cache.Selected == nil {
		return 0, storage.ErrLogNotSelected
	}
	return f.Cache.HeadIndex(otype), nil
}

func (f MassifFinder) Native(massifIndex uint32, otype storage.ObjectType) (any, error) {
	return nil, storage.ErrNativeNotImplemented
}

func (f MassifFinder) GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error) {
	data, err := f.Cache.GetData(ctx, massifIndex)
	if err != nil {
		return nil, err
	}
	return NewMassifContext(data)
}

func (f MassifFinder) GetHeadContext(ctx context.Context) (*massifs.MassifContext, error) {
	data, err := f.GetHeadMassif(ctx)
	if err != nil {
		return nil, err
	}
	return NewMassifContext(data)
}

func (f MassifFinder) GetStart(ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error) {
	start, err := f.Cache.GetStart(ctx, massifIndex)
	if err != nil {
		return nil, err
	}
	return start, nil
}

func (f MassifFinder) GetMassif(ctx context.Context, massifIndex uint32, opts ...massifs.Option) ([]byte, error) {
	data, err := f.Cache.GetData(ctx, massifIndex)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f MassifFinder) GetHeadMassif(ctx context.Context, opts ...massifs.Option) ([]byte, error) {
	return f.GetMassif(ctx, f.Cache.Selected.HeadMassifIndex, opts...)
}

func (f MassifFinder) GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error) {
	_, _, checkpt, err := f.Cache.Get(ctx, massifIndex)
	if err != nil {
		return nil, err
	}
	return checkpt, nil
}

func (f MassifFinder) GetHeadCheckpoint(ctx context.Context) (*massifs.Checkpoint, error) {
	return f.GetCheckpoint(ctx, f.Cache.Selected.HeadSealIndex)
}

func (f *MassifFinder) ReplaceVerifiedContext(ctx context.Context, vc *massifs.VerifiedContext) error {
	if f.Cache.Selected == nil {
		return storage.ErrLogNotSelected
	}
	return f.Cache.ReplaceVerified(vc)
}

func (f *MassifFinder) SelectLog(ctx context.Context, logId storage.LogID) error {
	if f.Cache == nil {
		return fmt.Errorf("massif cache not initialized")
	}
	if logId == nil {
		return fmt.Errorf("logId cannot be nil")
	}
	return f.PopulateCache(ctx, logId)

}

func (f *MassifFinder) PopulateCache(ctx context.Context, logID storage.LogID) error {
	var err error

	if err := f.Cache.SelectLog(logID); err != nil {
		return fmt.Errorf("failed to select log %x: %w", logID, err)
	}

	// Note: the explicit provision of MassifFilename only serves to locate the directory
	massifsDir, err := f.Opts.PrefixProvider.Prefix(logID, storage.ObjectMassifData)
	if err != nil {
		return fmt.Errorf("failed to get massif prefix for log %x: %w", logID, err)
	}
	checkPointsDir, err := f.Opts.PrefixProvider.Prefix(logID, storage.ObjectCheckpoint)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint prefix for log %x: %w", logID, err)
	}

	var massifPaths []string
	var checkpointPaths []string

	massifPaths, err = NewSuffixDirLister(f.Opts.MassifExtension).ListFiles(massifsDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to list massif files in %s: %w", massifsDir, err)
	}

	checkpointPaths, err = NewSuffixDirLister(f.Opts.SealExtension).ListFiles(checkPointsDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to list checkpoint files in %s: %w", checkPointsDir, err)
	}

	for _, e := range []struct {
		paths []string
		ty    storage.ObjectType
	}{
		{paths: massifPaths, ty: storage.ObjectMassifData},
		{paths: checkpointPaths, ty: storage.ObjectCheckpoint},
	} {
		for _, storagePath := range e.paths {

			err = f.Cache.Prime(ctx, storagePath, e.ty)
			if err != nil {
				return fmt.Errorf("failed to prime massif data for %s: %w", storagePath, err)
			}
		}
	}
	return nil
}
