//go:build disable

package storage

import (
	"context"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

func (f *MassifFinder) GetHeadContextVerified(ctx context.Context, opts ...massifs.Option) (*massifs.VerifiedContext, error) {
	massifIndex, err := f.HeadIndex(ctx, storage.ObjectMassifData)
	if err != nil {
		return nil, err
	}
	return f.getContextVerified(ctx, massifIndex, opts...)
}

func (f *MassifFinder) GetContextVerified(ctx context.Context, massifIndex uint32, opts ...massifs.Option) (*massifs.VerifiedContext, error) {
	return f.getContextVerified(ctx, massifIndex, opts...)
}

func (f *MassifFinder) getContextVerified(ctx context.Context, massifIndex uint32, opts ...massifs.Option) (*massifs.VerifiedContext, error) {
	verifyOpts := massifs.VerifyOptions{
		CBORCodec:    f.Opts.CBORCodec,
		COSEVerifier: f.Opts.COSEVerifier,
	}

	for _, opt := range opts {
		opt(&verifyOpts)
	}

	return f.getContextVerifiedOptioned(ctx, massifIndex, &verifyOpts)
}

func (f *MassifFinder) getContextVerifiedOptioned(ctx context.Context, massifIndex uint32, options *massifs.VerifyOptions) (*massifs.VerifiedContext, error) {
	var err error

	// Get the massif context
	mc, err := f.GetMassifContext(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	// If the checkpoint is not provided, fetch it.
	//
	// If the caller has this locally and has configured the verifier for it,
	// they do not need to use TrustedBaseState, except as a convenience to
	// check consistency with two states at once.
	if options.Check == nil {
		options.Check, err = f.GetCheckpoint(ctx, massifIndex)
		if err != nil {
			return nil, err
		}
	}

	// Verify the context
	return mc.VerifyContext(ctx, *options)
}
