package storage

import (
	"os"
	"path/filepath"
	"testing"

	commoncbor "github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	fsstorage "github.com/robinbryce/go-merklelog-fs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
)

type TestContext struct {
	mmrtesting.TestGenerator
	Cfg    *TestOptions
	Log    logger.Logger
	Finder *fsstorage.MassifFinder
}

type TestOptions struct {
	mmrtesting.TestOptions
	// Container  string // can be "" defaults to TestLablePrefix
	DebugLevel string // defaults to NOOP
	FinderOpts fsstorage.Options
}

func NewDefaultTestContext(t *testing.T, opts ...massifs.Option) *TestContext {
	opts = append([]massifs.Option{mmrtesting.WithDefaults()}, opts...)
	return NewTestContext(t, nil, nil, opts...)
}

// Satisfy the provider tests interface

func (c *TestContext) GetTestCfg() mmrtesting.TestOptions {
	return c.Cfg.TestOptions
}

func (c *TestContext) GetT() *testing.T {
	return c.TestGenerator.T
}

func (c *TestContext) NewMassifContextReader(opts massifs.StorageOptions) (massifs.MassifContextReader, error) {
	return c.NewNativeMassifFinder(opts)
}

// end interface implementation

func NewTestContext(t *testing.T, c *TestContext, cfg *TestOptions, opts ...massifs.Option) *TestContext {
	if cfg == nil {
		cfg = &TestOptions{}
	}
	for _, opt := range opts {
		opt(&cfg.FinderOpts)
		opt(&cfg.TestOptions)
		opt(cfg)
	}

	logLevel := cfg.DebugLevel
	if logLevel == "" {
		logLevel = "NOOP"
		cfg.DebugLevel = logLevel
	}
	logger.New(logLevel)

	if c == nil {
		c = &TestContext{
			Cfg: cfg,
		}
	}
	c.TestGenerator.Init(t, &cfg.TestOptions)

	c.Log = logger.Sugar.WithServiceName(cfg.TestOptions.TestLabelPrefix)

	var err error
	c.Finder, err = fsstorage.NewMassifFinder(&cfg.FinderOpts)
	if err != nil {
		t.Fatalf("failed to create MassifFinder: %v", err)
	}

	return c
}

func (c *TestContext) NewNativeMassifFinder(opts massifs.StorageOptions) (*fsstorage.MassifFinder, error) {
	var err error
	if opts.CBORCodec == nil {
		var codec commoncbor.CBORCodec
		codec, err = massifs.NewCBORCodec()
		if err != nil {
			return nil, err
		}
		opts.CBORCodec = &codec
	}
	if opts.PathProvider == nil {
		opts.PathProvider = datatrails.NewFixedPaths(opts.LogID)
	}
	azopts := fsstorage.Options{}
	azopts.Options.StorageOptions = opts
	return fsstorage.NewMassifFinder(&azopts)
}

func (c *TestContext) GetLog() logger.Logger { return c.Log }

func (c *TestContext) DeleteBlobsByPrefix(blobPrefixPath string) {
	// Basic safety checks
	if blobPrefixPath == "" || blobPrefixPath == "/" {
		c.T.Fatalf("refusing to delete directories under empty path or '/'")
	}

	entries, err := os.ReadDir(blobPrefixPath)
	if err != nil {
		c.T.Fatalf("reading directory %q: %w", blobPrefixPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdirPath := filepath.Join(blobPrefixPath, entry.Name())
			if err := os.RemoveAll(subdirPath); err != nil {
				c.T.Fatalf("removing directory %q: %w", subdirPath, err)
			}
		}
	}
}
