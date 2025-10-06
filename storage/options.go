package storage

import (
	"os"

	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"

	// "github.com/forestrie/go-merklelog-fs/storage"
	commoncbor "github.com/forestrie/go-merklelog/massifs/cbor"
)

const (
	DefaultSealExt      = storage.V1MMRExtSep + storage.V1MMRSealSignedRootExt
	DefaultMassifExt    = storage.V1MMRExtSep + storage.V1MMRMassifExt
	DefaultMassifHeight = 14
)

var DefaultLogID = []byte{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

type FSOptions struct {
	RootDir         string
	CreateRootDir   bool
	MassifFile      string
	CheckpointFile  string
	SealExtension   string // e.g. ".sth"
	MassifExtension string // e.g. ".log"
	ReadOpener      Opener
	WriteOpener     WriteOpener
	FileCreateMode  os.FileMode
	DirCreateMode   os.FileMode
}

type Options struct {
	massifs.StorageOptions
	FSOptions
}

func (opts *Options) Clone() Options {
	return *opts
}

func WithRootDir(dir string) massifs.Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.RootDir = dir
		}
	}
}

func WithCreateRootDir() massifs.Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.CreateRootDir = true
		}
	}
}

func WithMassifFile(fileName string) massifs.Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.MassifFile = fileName
		}
	}
}

func WithCheckpointFile(fileName string) massifs.Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.CheckpointFile = fileName
		}
	}
}

func (opts *Options) FillDefaults() error {
	var err error

	if opts.MassifHeight == 0 {
		opts.MassifHeight = DefaultMassifHeight
	}

	if opts.SealExtension == "" {
		opts.SealExtension = DefaultSealExt
	}

	if opts.MassifExtension == "" {
		opts.MassifExtension = DefaultMassifExt
	}

	if opts.CBORCodec == nil {
		var codec commoncbor.CBORCodec
		if codec, err = massifs.NewCBORCodec(); err != nil {
			return err
		}
		opts.CBORCodec = &codec
	}

	if opts.FileCreateMode == 0 {
		opts.FileCreateMode = 0644
	}
	if opts.DirCreateMode == 0 {
		opts.DirCreateMode = 0755
	}

	// if opts.PrefixProvider == nil {
	// 	return nil, fmt.Errorf("a prefix provider is required")
	// }

	if opts.MassifExtension == "" {
		opts.MassifExtension = DefaultMassifExt
	}
	if opts.ReadOpener == nil {
		opts.ReadOpener = NewFileOpener()
	}
	if opts.WriteOpener == nil {
		opts.WriteOpener = NewDefaultWriteOpener(opts.FileCreateMode)
	}
	return nil
}

func NewOptionsWithDefaults(parent *Options) (*Options, error) {
	opts := &Options{}
	if parent != nil {
		*opts = *parent
	}

	if err := opts.FillDefaults(); err != nil {
		return nil, err
	}

	return opts, nil
}

type CheckpointOptions struct{}
