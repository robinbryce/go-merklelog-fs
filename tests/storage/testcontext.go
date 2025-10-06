package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/forestrie/go-merklelog/massifs"
	fsstorage "github.com/forestrie/go-merklelog-fs/storage"
	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
)

type TestContext struct {
	mmrtesting.TestContext[*TestContext]
	Cfg *TestOptions
	Log logger.Logger
}

type TestOptions struct {
	mmrtesting.TestOptions
	// Container  string // can be "" defaults to TestLabelPrefix
	DebugLevel string // defaults to NOOP
	FinderOpts fsstorage.Options
}

func NewDefaultTestContext(t *testing.T, opts ...massifs.Option) *TestContext {
	opts = append([]massifs.Option{mmrtesting.WithDefaults()}, opts...)
	return NewTestContext(t, nil, opts...)
}

// Satisfy the provider tests interface

func (c *TestContext) GetTestCfg() mmrtesting.TestOptions {
	return c.Cfg.TestOptions
}

func (c *TestContext) GetT() *testing.T {
	return c.T
}

func NewTestContext(t *testing.T, cfg *TestOptions, opts ...massifs.Option) *TestContext {

	if cfg == nil {
		cfg = &TestOptions{}
	}
	for _, opt := range opts {
		opt(&cfg.TestOptions)
		opt(cfg)
	}

	c := &TestContext{
		Cfg: cfg,
	}
	c.init(t, cfg)
	return c
}

func (c *TestContext) init(t *testing.T, cfg *TestOptions) {

	cfg.EnsureDefaults(t)

	c.Emulator = c

	logger.New(cfg.DebugLevel)

	c.TestContext.Init(t, &cfg.TestOptions)
	c.Cfg = cfg

	c.Log = logger.Sugar.WithServiceName(cfg.TestOptions.TestLabelPrefix)
}

func (c *TestContext) DeleteByStoragePrefix(blobPrefixPath string) {
	// Basic safety checks
	if blobPrefixPath == "" || blobPrefixPath == "/" {
		c.T.Fatalf("refusing to delete directories under empty path or '/'")
	}

	storagePath := filepath.Join(c.Cfg.RootDir, blobPrefixPath)

	entries, err := os.ReadDir(storagePath)
	if err != nil {
		if !os.IsNotExist(err) {
			c.T.Fatalf("reading directory %q: %v", storagePath, err)
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdirPath := filepath.Join(storagePath, entry.Name())
			if err := os.RemoveAll(subdirPath); err != nil {
				c.T.Fatalf("removing directory %q: %v", subdirPath, err)
			}
		}
	}
}
