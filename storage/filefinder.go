package storage

import (
	"context"
	"fmt"

	commoncbor "github.com/datatrails/go-datatrails-common/cbor"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
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

func identifyLog(ctx context.Context, storagePath string) (storage.LogID, error) {
	if storagePath == "" {
		return storage.LogID(DefaultLogID), nil
	}
	logID, err := datatrails.IdentifyLogTenantID(ctx, storagePath)
	if err != nil {
		return nil, err
	}
	if logID != nil {
		return logID, nil
	}
	return storage.LogID(DefaultLogID), nil
}

type Options struct {
	filecache.Options

	// MassifFilename is the name of the massif file, e.g. "massif.log"
	// if this is set, Dir is ignored
	MassifFilename string
	// SealFilename is the name of the seal file, e.g. "massif.sth"
	// if this is set, SealDir is ignored, and SealDir does not default to Dir
	SealFilename string

	// Dir is the directory where the massif files are stored
	Dir string
	// SealDir is the directory where the seal files are stored, if different from Dir
	SealDir string

	SealExtension   string // e.g. ".sth"
	MassifExtension string // e.g. ".log"
}

type CheckpointOptions struct{}

func WithMassifFilename(massifFilename string) filecache.Option {
	return func(a any) {
		if opts, ok := a.(*Options); ok {
			opts.MassifFilename = massifFilename
		}
	}
}

type MassifFinder struct {
	Opts  *Options
	Cache *filecache.Cache
}

/*
	cache, err := dircache.NewLogDirCache(
		dircache.WithMassifLister(NewSuffixDirLister(f.cfg.MassifExtension)),
		dircache.WithSealLister(NewSuffixDirLister(f.cfg.SealExtension)),
		dircache.WithMassifHeight(f.cfg.MassifHeight),
		dircache.WithCBORCodec(f.cfg.CBORCodec),
		dircache.WithOpener(NewFileOpener()),
		dircache.WithLogger(f.cfg.Log),
	)*/

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

	if f.Opts.IdentifyLog == nil {
		f.Opts.IdentifyLog = identifyLog
	}
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

	if f.Opts.Dir == "" && f.Opts.MassifFilename == "" {
		return fmt.Errorf("either Dir or MassifFilename must be set")
	}

	if f.Opts.SealDir == "" && f.Opts.SealFilename == "" {
		f.Opts.SealDir = f.Opts.Dir
	}

	if f.Opts.SealExtension == "" {
		f.Opts.SealExtension = DefaultSealExt
	}

	if f.Opts.MassifExtension == "" {
		f.Opts.MassifExtension = DefaultMassifExt
	}

	f.Cache, err = filecache.NewCache(f.Opts.Options)
	if err != nil {
		return fmt.Errorf("failed to create massif cache: %w", err)
	}

	return nil
}

func (f *MassifFinder) Prepare(ctx context.Context) error {
	return f.populateCache(ctx)
}

func (f *MassifFinder) SelectLog(logId storage.LogID, pathProvider storage.PathProvider) error {
	if f.Cache == nil {
		return fmt.Errorf("massif cache not initialized")
	}
	if logId == nil {
		return fmt.Errorf("logId cannot be nil")
	}
	if pathProvider == nil {
		pathProvider = f.Opts.PathProvider
	}
	if err := f.Cache.SelectLog(logId, pathProvider); err != nil {
		return fmt.Errorf("failed to select log %s: %w", logId, err)
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
	/*
		if c.Selected == nil {
			return nil, storage.ErrLogNotSelected
		}

		   switch otype {
		   case storage.ObjectMassifStart:

		   	if massifIndex < f.Cache.Selected.FirstMassifIndex || massifIndex > f.Cache.Selected.HeadMassifIndex {
		   		return nil, storage.ErrDoesNotExist
		   	}
		   	// TODO: index -> storagepath
		   	return nil, storage.ErrNativeNotImplemented

		   case storage.ObjectMassifData:

		   	if massifIndex < f.Cache.Selected.FirstMassifIndex || massifIndex > f.Cache.Selected.HeadMassifIndex {
		   		return nil, storage.ErrDoesNotExist
		   	}
		   	// TODO: index -> storagepath
		   	return nil, storage.ErrNativeNotImplemented

		   case storage.ObjectCheckpoint:

		   	if massifIndex < f.Cache.Selected.FirstSealIndex || massifIndex > f.Cache.Selected.HeadSealIndex {
		   		return nil, storage.ErrDoesNotExist
		   	}
		   	// TODO: index -> storagepath
		   	return nil, storage.ErrNativeNotImplemented

		   default:

		   		return nil, fmt.Errorf("unsupported object type %v", otype)
		   	}
	*/
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

func (f *MassifFinder) log(msg string, args ...any) {
	if f.Opts.Log == nil {
		return
	}
	f.Opts.Log.Infof(msg, args...)
}

func (f *MassifFinder) populateCache(ctx context.Context) error {
	var err error

	var massifPaths []string
	var checkpointPaths []string

	massifPaths, err = NewSuffixDirLister(f.Opts.MassifExtension).ListFiles(f.Opts.Dir)
	if err != nil {
		return fmt.Errorf("failed to list massif files in %s: %w", f.Opts.Dir, err)
	}
	// if there is a specific massif filename, use that and also use it first
	if len(f.Opts.MassifFilename) > 0 {
		massifPaths = append([]string{f.Opts.MassifFilename}, massifPaths...)
	}

	checkpointPaths, err = NewSuffixDirLister(f.Opts.SealExtension).ListFiles(f.Opts.SealDir)
	if err != nil {
		return fmt.Errorf("failed to list massif files in %s: %w", f.Opts.Dir, err)
	}
	// if there is a specific massif filename, use that and also use it first
	if len(f.Opts.SealFilename) > 0 {
		checkpointPaths = append([]string{f.Opts.SealFilename}, checkpointPaths...)
	}

	for _, e := range []struct {
		paths []string
		ty    storage.ObjectType
	}{
		{paths: massifPaths, ty: storage.ObjectMassifData},
		{paths: checkpointPaths, ty: storage.ObjectCheckpoint},
	} {
		for _, storagePath := range e.paths {

			pathProvider := NewPathProviderFromPath(storagePath)
			if err := f.Cache.SelectLog(pathProvider.CurrentLogID, pathProvider); err != nil {
				return fmt.Errorf("failed to select log %s: %w", storagePath, err)
			}

			err = f.Cache.Prime(ctx, storagePath, e.ty)
			if err != nil {
				return fmt.Errorf("failed to prime massif data for %s: %w", storagePath, err)
			}
		}
	}
	return nil
}
