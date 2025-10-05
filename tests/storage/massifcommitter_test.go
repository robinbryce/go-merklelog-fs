package storage

import (
	"testing"

	"github.com/forestrie/go-merklelog/massifs/storage"
	fsstorage "github.com/robinbryce/go-merklelog-fs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
	"github.com/stretchr/testify/require"
)

func NewLogBuilderFactory(tc *TestContext) mmrtesting.LogBuilder {
	fsopts := fsstorage.Options{}
	if fsopts.RootDir == "" {
		fsopts.RootDir = tc.Cfg.RootDir
	}

	store, err := fsstorage.NewStore(tc.T.Context(), fsopts)
	require.NoError(tc.T, err)

	builder := mmrtesting.LogBuilder{
		LeafGenerator: mmrtesting.LeafGenerator{
			Generator: func(logID storage.LogID, base, i uint64) any {
				return tc.G.GenerateLeafContent(logID, base, i)
			},
			Encoder: func(a any) mmrtesting.AddLeafArgs {
				return tc.G.EncodeLeafForAddition(a)
			},
		},
		DeleteLog:          tc.DeleteLog,
		SelectLog:          store.SelectLog,
		ObjectReader:       store,
		ObjectWriter:       store,
		ObjectReaderWriter: store,
	}
	return builder

}

func NewBuilderFactory(tc *TestContext) providers.BuilderFactory {
	return func() mmrtesting.LogBuilder {
		return NewLogBuilderFactory(tc)
	}
}

func TestMassifCommitter_firstMassif(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_firstMassif"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterFirstMassifTest(tc, factory)
}

func TestMassifCommitter_addFirstTwoLeaves(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_addFirstTwoLeaves"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc, factory)
}

func TestMassifCommitter_extendAndCommitFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_extendAndCommitFirst"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc, factory)
}

func TestMassifCommitter_completeFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_completeFirst"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterCompleteFirstTest(tc, factory)
}

func TestMassifCommitter_overfillSafe(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_overfillSafe"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterOverfillSafeTest(tc, factory)
}

func TestMassifCommitter_threeMassifs(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_threeMassifs"))
	sc := NewBuilderFactory(tc)
	providers.StorageMassifCommitterThreeMassifsTest(tc, sc)
}
