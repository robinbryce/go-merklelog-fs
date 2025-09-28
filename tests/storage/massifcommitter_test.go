package storage

import (
	"testing"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	fsstorage "github.com/robinbryce/go-merklelog-fs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
	"github.com/stretchr/testify/require"
)

func NewLogBuilderFactory(tc *TestContext, opts massifs.StorageOptions) mmrtesting.LogBuilder {
	fsopts := tc.defaultStoreOpts(opts)

	store, err := fsstorage.NewMassifStore(tc.T.Context(), fsopts)
	require.NoError(tc.T, err)
	committer, err := fsstorage.NewMassifCommitterStore(tc.T.Context(), store, fsopts)
	require.NoError(tc.T, err)

	builder := mmrtesting.LogBuilder{
		LeafGenerator: mmrtesting.LeafGenerator{
			LogID: opts.LogID,
			Generator: func(logID storage.LogID, base, i uint64) any {
				return tc.G.GenerateLeafContent(logID, base, i)
			},
			Encoder: func(a any) mmrtesting.AddLeafArgs {
				return tc.G.EncodeLeafForAddition(a)
			},
		},
		DeleteLog:       tc.DeleteLog,
		MassifCommitter: committer,
		MassifSealer:    store,
		ObjectStore:     store,
	}
	return builder

}

func NewStorageTestContext(tc *TestContext) *providers.StorageMassifCommitterContext {
	sc := &providers.StorageMassifCommitterContext{
		BuilderFactory: func(opts massifs.StorageOptions) mmrtesting.LogBuilder {
			return NewLogBuilderFactory(tc, opts)
		},
	}
	return sc
}

func TestMassifCommitter_firstMassif(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_firstMassif"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterFirstMassifTest(tc, sc)
}

func TestMassifCommitter_addFirstTwoLeaves(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_addFirstTwoLeaves"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc, sc)
}

func TestMassifCommitter_extendAndCommitFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_extendAndCommitFirst"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc, sc)
}

func TestMassifCommitter_completeFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_completeFirst"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterCompleteFirstTest(tc, sc)
}

func TestMassifCommitter_overfillSafe(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_overfillSafe"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterOverfillSafeTest(tc, sc)
}

func TestMassifCommitter_threeMassifs(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_threeMassifs"))
	sc := NewStorageTestContext(tc)
	providers.StorageMassifCommitterThreeMassifsTest(tc, sc)
}
