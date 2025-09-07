package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	fsstorage "github.com/robinbryce/go-merklelog-fs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/stretchr/testify/require"
)

type TestContext struct {
	mmrtesting.TestContext[*TestContext, *TestContext]
	Cfg *TestOptions
	Log logger.Logger
}

type TestOptions struct {
	mmrtesting.TestOptions
	// Container  string // can be "" defaults to TestLablePrefix
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
	c.Factory = c

	logger.New(cfg.DebugLevel)

	c.TestContext.Init(t, &cfg.TestOptions)
	c.Cfg = cfg

	c.Log = logger.Sugar.WithServiceName(cfg.TestOptions.TestLabelPrefix)
}

func (c *TestContext) defaultStoreOpts(opts massifs.StorageOptions) fsstorage.Options {
	fsopts := fsstorage.Options{
		StorageOptions: opts,
	}

	// Create PrefixProvider for filesystem storage
	// Use a default directory path for testing
	prefixProvider := &fsstorage.PrefixProvider{
		Dir: "/tmp/forestrie-test", // Use temporary directory for tests
	}
	fsopts.PrefixProvider = prefixProvider

	// Apply defaults if needed
	if fsopts.LogID == nil {
		fsopts.LogID = c.Cfg.TestOptions.LogID
	}
	if fsopts.MassifHeight == 0 {
		fsopts.MassifHeight = c.Cfg.TestOptions.MassifHeight
	}
	if fsopts.CommitmentEpoch == 0 {
		fsopts.CommitmentEpoch = c.Cfg.TestOptions.CommitmentEpoch
	}
	if fsopts.PathProvider == nil {
		fsopts.PathProvider = datatrails.NewPathProvider(opts.LogID)
	}
	return fsopts
}

func (c *TestContext) NewMassifGetter(opts massifs.StorageOptions) (massifs.MassifContextGetter, error) {
	fsopts := c.defaultStoreOpts(opts)

	store, err := fsstorage.NewMassifStore(c.T.Context(), fsopts)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// Implement TestContext[FilesystemMetadata] interface
func (c *TestContext) NewMassifCommitter(opts massifs.StorageOptions) (*massifs.MassifCommitter[massifs.HeadReplacer], error) {

	fsopts := c.defaultStoreOpts(opts)

	store, err := fsstorage.NewMassifStore(c.T.Context(), fsopts)
	require.NoError(c.T, err, "failed to initialize caching store")

	return fsstorage.NewMassifCommitter(c.T.Context(), store, fsopts)
}

func (c *TestContext) NewMassifCommitterStore(opts massifs.StorageOptions) (*massifs.MassifCommitter[massifs.CommitterStore], error) {

	fsopts := c.defaultStoreOpts(opts)

	provider, err := fsstorage.NewMassifStore(c.T.Context(), fsopts)
	require.NoError(c.T, err, "failed to initialize caching store")

	committer, err := fsstorage.NewMassifCommitterStore(c.T.Context(), provider, fsopts)
	require.NoError(c.T, err, "failed to create MassifCommitter")

	return committer, nil
}

func (c *TestContext) NewCommitterStore(opts massifs.StorageOptions) (massifs.CommitterStore, error) {

	fsopts := c.defaultStoreOpts(opts)

	// cachingStore := fsstorage.CachingStore{}
	// err := cachingStore.Init(c.T.Context(), &fsopts)
	// if err != nil {
	// 	return nil, err
	// }
	// return &cachingStore, nil

	provider, err := fsstorage.NewMassifStore(c.T.Context(), fsopts)
	require.NoError(c.T, err, "failed to initialize caching store")
	return provider, nil
}

func (c *TestContext) PadWithNumberedLeaves(data []byte, first, n int) []byte {
	return c.G.PadWithNumberedLeaves(data, first, n)
}

func (c *TestContext) EncodeLeafForAddition(a any) mmrtesting.AddLeafArgs {
	return c.G.EncodeLeafForAddition(a)
}

func (c *TestContext) GenerateLeafContent(logID storage.LogID) any {
	return c.G.GenerateLeafContent(logID)
}

func (c *TestContext) DeleteByStoragePrefix(blobPrefixPath string) {
	// Basic safety checks
	if blobPrefixPath == "" || blobPrefixPath == "/" {
		c.T.Fatalf("refusing to delete directories under empty path or '/'")
	}

	entries, err := os.ReadDir(blobPrefixPath)
	if err != nil {
		c.T.Fatalf("reading directory %q: %v", blobPrefixPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdirPath := filepath.Join(blobPrefixPath, entry.Name())
			if err := os.RemoveAll(subdirPath); err != nil {
				c.T.Fatalf("removing directory %q: %v", subdirPath, err)
			}
		}
	}
}

func (c *TestContext) GetLog() logger.Logger { return c.Log }
