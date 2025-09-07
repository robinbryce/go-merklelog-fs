package storage

import (
	"testing"

	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
)

// Phase 4 unified tests - using MassifCommitter[FilesystemMetadata] directly
// All tests now use the generic provider tests with filesystem-specific metadata type

func TestMassifCommitter_firstMassif(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_firstMassif"))
	providers.StorageMassifCommitterFirstMassifTest(tc)
}

func TestMassifCommitter_addFirstTwoLeaves(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_addFirstTwoLeaves"))
	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc)
}

func TestMassifCommitter_extendAndCommitFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_extendAndCommitFirst"))
	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc)
}

func TestMassifCommitter_completeFirst(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_completeFirst"))
	providers.StorageMassifCommitterCompleteFirstTest(tc)
}

func TestMassifCommitter_overfillSafe(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_overfillSafe"))
	providers.StorageMassifCommitterOverfillSafeTest(tc)
}

func TestMassifCommitter_threeMassifs(t *testing.T) {
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestUnifiedMassifCommitter_threeMassifs"))
	providers.StorageMassifCommitterThreeMassifsTest(tc)
}
