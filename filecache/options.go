package filecache

import (
	commoncbor "github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type Options struct {
	massifs.StorageOptions
	// CBORCodec    *commoncbor.CBORCodec
	// MassifHeight uint8 // e.g. 14
	IdentifyLog storage.IdentifyLogFunc
	Opener      Opener
	Log         logger.Logger
}

// Options for configuring the LogDirCache. Implementations type assert to Options
// and if that fails they ignore the options
type Option func(any)

func WithLogIdentifier(identifyLog storage.IdentifyLogFunc) Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.IdentifyLog = identifyLog
		}
	}
}

func WithCBORCodec(codec *commoncbor.CBORCodec) Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.StorageOptions.CBORCodec = codec
		}
	}
}

func WithMassifHeight(height uint8) Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.StorageOptions.MassifHeight = height
		}
	}
}

func WithOpener(opener Opener) Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.Opener = opener
		}
	}
}

func WithLogger(log logger.Logger) Option {
	return func(a any) {
		if o, ok := a.(*Options); ok {
			o.Log = log
		}
	}
}
