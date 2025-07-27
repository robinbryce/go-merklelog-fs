//go:build disable

// this has to be separate from the other tests because it imports the mocks package

package storage

import (
	"context"
	"crypto/elliptic"
	"errors"
	"strings"
	"testing"

	"github.com/datatrails/go-datatrails-common/cose"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/mocks"
	"github.com/datatrails/go-datatrails-merklelog/mmr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestLocalMassifReaderGetVerifiedContext ensures the conistency checks are
// performed according to the avialable data and the provide options
//
// The major log verification scenarios tested are:
//  1. The remote massif data has been tampered to include, modify or exclude a leaf.
//  2. The remote massif data has been extended inconsistently with respect to ealier un-tampered data.
//  3. The remote massif data and the latest remote seal have been tampered, but a previously "known good" seal for the massif is available locally.
//
// The signing key verification scenarios tested are:
//  1. It can be absent
//  2. It can be present and match the sealing public key
//  3. It can be present and NOT match the sealing public key
func TestLocalMassifReaderGetVerifiedContext(t *testing.T) {
	logger.New("TestLocalMassifReaderGetVerifiedContext")
	defer logger.OnExit()

	tc := massifs.NewLocalMassifReaderTestContext(
		t, logger.Sugar, "TestLocalMassifReaderGetVerifiedContext")

	tenantId0 := tc.G.NewTenantIdentity()
	tenantId1SealBehindLog := tc.G.NewTenantIdentity()
	tenantId2TamperedLogUpdate := tc.G.NewTenantIdentity()
	tenantId3InconsistentLogUpdate := tc.G.NewTenantIdentity()
	tenantId4RemoteInconsistentWithTrustedSeal := tc.G.NewTenantIdentity()
	tenantId5TrustedPublicKeyMismatch := tc.G.NewTenantIdentity()

	allTenants := []string{tenantId0, tenantId1SealBehindLog, tenantId2TamperedLogUpdate, tenantId3InconsistentLogUpdate, tenantId4RemoteInconsistentWithTrustedSeal, tenantId5TrustedPublicKeyMismatch}

	massifHeight := uint8(8)
	tc.CreateLog(tenantId0, massifHeight, 3)
	tc.CreateLog(tenantId1SealBehindLog, massifHeight, 3)
	tc.AddLeavesToLog(tenantId1SealBehindLog, massifHeight, 1)
	tc.CreateLog(tenantId2TamperedLogUpdate, massifHeight, 3)
	tc.AddLeavesToLog(tenantId2TamperedLogUpdate, massifHeight, 2)
	tc.CreateLog(tenantId3InconsistentLogUpdate, massifHeight, 3)
	tc.AddLeavesToLog(tenantId3InconsistentLogUpdate, massifHeight, 3)
	tc.CreateLog(tenantId4RemoteInconsistentWithTrustedSeal, massifHeight, 3)
	tc.AddLeavesToLog(tenantId4RemoteInconsistentWithTrustedSeal, massifHeight, 4)
	tc.CreateLog(tenantId5TrustedPublicKeyMismatch, massifHeight, 3)
	tc.AddLeavesToLog(tenantId5TrustedPublicKeyMismatch, massifHeight, 5)

	// sizeBeforeLeaves returns the size of the massif before the leaves provded number of leaves were added
	sizeBeforeLeaves := func(mc *massifs.MassifContext, leavesBefore uint64) uint64 {
		mmrSize := mc.RangeCount()
		leafCount := mmr.LeafCount(mmrSize)
		oldLeafCount := leafCount - leavesBefore
		mmrSizeOld := mmr.FirstMMRSize(mmr.MMRIndex(oldLeafCount - 1))
		return mmrSizeOld
	}

	findMassif := func(identifier string, massifIndex uint64) (*massifs.MassifContext, error) {
		for _, tenantId := range allTenants {
			if !strings.Contains(identifier, tenantId) {
				continue
			}
			mc, err := tc.AzuriteReader.GetMassif(t.Context(), tenantId, massifIndex)
			if err != nil {
				return nil, err
			}
			return &mc, nil
		}
		return nil, massifs.ErrMassifNotFound
	}

	tamperNode := func(mc *massifs.MassifContext, mmrIndex uint64) {
		require.GreaterOrEqual(t, mmrIndex, mc.Start.FirstIndex)
		i := mmrIndex - mc.Start.FirstIndex
		logData := mc.Data[mc.LogStart():]
		tamperedBytes := []byte{0x0D, 0x0E, 0x0A, 0x0D, 0x0B, 0x0E, 0x0E, 0x0F}
		copy(logData[i*massifs.LogEntryBytes:i*massifs.LogEntryBytes+8], tamperedBytes)
	}

	/*
		sealV0 := func(
			mc *massifs.MassifContext, mmrSize uint64, tenantIdentity string, massifIndex uint32,
		) (*cose.CoseSign1Message, massifs.MMRState, error) {
			root, err := mmr.GetRoot(mmrSize, mc, sha256.New())
			if err != nil {
				return nil, massifs.MMRState{}, err
			}
			signed, state, err := tc.SignedState(tenantIdentity, uint64(massifIndex), massifs.MMRState{
				MMRSize: mmrSize, LegacySealRoot: root,
			})
			// put the root back, because the benefit of the "last good seal"
			// consistency check does not require access to the log data.
			state.LegacySealRoot = root
			return signed, state, err
		}*/
	seal := func(
		mc *massifs.MassifContext, mmrIndex uint64, tenantIdentity string, massifIndex uint32,
	) (*cose.CoseSign1Message, massifs.MMRState, error) {
		peaks, err := mmr.PeakHashes(mc, mmrIndex)
		if err != nil {
			return nil, massifs.MMRState{}, err
		}
		signed, state, err := tc.SignedState(tenantIdentity, uint64(massifIndex), massifs.MMRState{
			Version: int(massifs.MMRStateVersion1),
			MMRSize: mmrIndex + 1, Peaks: peaks,
		})
		// put the root back, because the benefit of the "last good seal"
		// consistency check does not require access to the log data.
		state.Peaks = peaks
		return signed, state, err
	}

	sg := *mocks.NewSealGetter(t)
	sg.On("GetSignedRoot", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(
			ctx context.Context, tenantIdentity string, massifIndex uint32,
			opts ...massifs.ReaderOption,
		) (*cose.CoseSign1Message, massifs.MMRState, error) {
			mc, err := findMassif(tenantIdentity, uint64(massifIndex))
			if err != nil {
				return nil, massifs.MMRState{}, err
			}
			switch tenantIdentity {
			case tenantId1SealBehindLog:
				// Common case: Return a seal that omits the last few leaves. In this case GetVerifiedContext should
				// return the root *inclusive* of the additional leaves, having verified the seal over only the original leaves.
				mmrSize := mc.RangeCount()
				leafCount := mmr.LeafCount(mmrSize)
				sealedLeafCount := leafCount - 8
				mmrSizeOld := mmr.FirstMMRSize(mmr.MMRIndex(sealedLeafCount - 1))
				require.GreaterOrEqual(t, mmrSizeOld, mc.Start.FirstIndex)

				return seal(mc, mmrSizeOld-1, tenantIdentity, massifIndex)
			case tenantId2TamperedLogUpdate:

				// We are simulating a situation where the locally available
				// root seal for the *earlier* massif data is correct, but the
				// updated (extended) log obtained from the remote (datatrails)
				// has been tampered with. To simulate this, we first obtain
				// the seal from the un-tampered log. Then, we tamper with the
				// log data. The verification will fail because the tampered log
				// will produce a different root.

				// Note that every time the GetSignedRoot mock or the ReadMassif
				// mock is called, the data is read fresh from the azurite
				// store.

				// Tampering a log requires updating all nodes after the
				// tampered node to maintain verifiability. In the case of a
				// delete, inclusion proofs for subsequent nodes would become
				// invalid. Therefore, in this simulation, we can simulate a
				// tampered *rebuilt* log, without actually re-building it, by
				// changing a peak node directly.

				// Detecting a gratuitously tampered leaf, where the tree is not
				// re-built, is the reason why third-party auditors are included
				// in the security model. In such cases, the seal would remain
				// unaffected, but nothing in the tampered log would verify against it.

				// It is important to note that all tamper scenarios still require
				// the attacker to have access to the signing key.

				// note; we don't specifically need to work with a mid massif
				// state here, we just do so to show this works for an arbitrary
				// seal point, and for alignment with consistency check tests

				mmrSizeOld := sizeBeforeLeaves(mc, 8)
				require.GreaterOrEqual(t, mmrSizeOld, mc.Start.FirstIndex)

				// Get the seal before applying the tamper
				msg, state, err := seal(mc, mmrSizeOld-1, tenantIdentity, massifIndex)
				if err != nil {
					return nil, massifs.MMRState{}, err
				}

				peakIndices := mmr.Peaks(mmrSizeOld - 1)
				// Remember, the peaks are *positions*
				peaks, err := mmr.PeakHashes(mc, mmrSizeOld-1)
				require.NoError(t, err)

				// Note: we take the *last* peak, because it corresponds to the
				// most recent log entries, but tampering any peak will cause
				// the verification to fail to fail
				tamperNode(mc, peakIndices[len(peakIndices)-1])

				peaks2, err := mmr.PeakHashes(mc, mmrSizeOld-1)
				require.NoError(t, err)

				assert.NotEqual(t, peaks, peaks2, "tamper did not change the root")

				// Now we can return the seal
				return msg, state, nil

			case tenantId3InconsistentLogUpdate:

				// In this case, the log is un-tampered, up to the seal, but the additions after the seal are inconsistent.

				// tamper *after* the seal
				mmrSizeOld := sizeBeforeLeaves(mc, 8)
				require.GreaterOrEqual(t, mmrSizeOld, mc.Start.FirstIndex)

				// Get the seal before applying the tamper
				msg, state, err := seal(mc, mmrSizeOld-1, tenantIdentity, massifIndex)
				if err != nil {
					return nil, massifs.MMRState{}, err
				}

				// this time, tamper a peak after the seal, this simulates the
				// case where the extension is inconsistent with the seal.
				peaks := mmr.Peaks(mc.RangeCount() - 1)

				// Note: we take the *last* peak, because it corresponds to the
				// most recent log entries. In this case we want the fresh
				// additions to the log to be inconsistent with the seal. Until
				// enough new entries are added, those new entries are only
				// dependent on the smallest sealed peak.

				// Remember, the peaks are *positions*
				tamperNode(mc, peaks[len(peaks)-1])

				// Now we can return the seal
				return msg, state, nil

			default:
				// Common case: the seal is the full extent of the massif
				return seal(mc, mc.RangeCount()-1, tenantIdentity, massifIndex)
			}
		})

	dc := mocks.NewDirCache(t)
	dc.On("Options", mock.Anything).Return(
		func() massifs.DirCacheOptions {
			return massifs.NewLogDirCacheOptions(
				massifs.ReaderOptions{},
				massifs.WithReaderOption(massifs.WithSealGetter(&sg)),
				massifs.WithReaderOption(massifs.WithCBORCodec(tc.RootSignerCodec)),
			)
		})
	dc.On("ResolveMassifDir", mock.Anything).Return(
		func(directory string) (string, error) {
			return directory, nil
		})
	dc.On("ReadMassifDirEntry", mock.Anything).Return(
		func(directory string) (massifs.DirCacheEntry, error) {
			dce := mocks.NewDirCacheEntry(t)
			dce.On("ReadMassif", mock.Anything, mock.Anything).Return(
				func(c massifs.DirCache, massifIndex uint64) (*massifs.MassifContext, error) {
					mc, err := findMassif(directory, massifIndex)
					if err != nil {
						return nil, err
					}
					switch directory {
					case tenantId2TamperedLogUpdate:

						// For the seal verification check, we ensure that the seal is
						// generated against the un tampered data in the GetSignedRoot
						// mock. Here, we ensure that all other observers see only the
						// tampered data.

						mmrSizeOld := sizeBeforeLeaves(mc, 8)
						require.GreaterOrEqual(t, mmrSizeOld, mc.Start.FirstIndex)
						peaks := mmr.Peaks(mmrSizeOld - 1)
						// remember, the peaks are *positions*
						tamperNode(mc, peaks[len(peaks)-1])

					case tenantId3InconsistentLogUpdate:
						// tamper *after* the seal
						// this time, tamper a peak after the seal, this simulates the
						// case where the extension is inconsistent with the seal.
						peaks := mmr.Peaks(mc.RangeCount() - 1)
						// Remember, the peaks are *positions*
						tamperNode(mc, peaks[len(peaks)-1])

					default:
					}
					return mc, nil
				})
			return dce, nil
		})

	// To provoke the case where the local, trusted, seal is inconsistent with
	// the remote seal & log, we play a bit of a trick. We get a seal for a
	// *tampered* log, then later, ALL the legit log data will otherwise verify
	// but will fail against the "trusted good seal". It is precisesly the
	// opposite of what we are protecting against in the real world, but it is
	// equivelent from a test perspective.

	mc, err := findMassif(tenantId4RemoteInconsistentWithTrustedSeal, 0)
	require.NoError(t, err)
	mmrSizeOld := sizeBeforeLeaves(mc, 8)
	require.GreaterOrEqual(t, mmrSizeOld, mc.Start.FirstIndex)
	peaks := mmr.Peaks(mmrSizeOld - 1)
	// remember, the peaks are *positions*
	tamperNode(mc, peaks[len(peaks)-1])

	// We  call this a fake good state because its actually tampered, and the
	// log is "good", but it has the same effect from a verification
	// perspective.
	_, fakeGoodState, err := seal(mc, mmrSizeOld-1, tenantId4RemoteInconsistentWithTrustedSeal, 0)
	require.NoError(t, err)

	fakeECKey := massifs.TestGenerateECKey(t, elliptic.P256())

	type logStates struct {
		mmrSize uint64
	}

	type args struct {
		tenantIdentity string
		// The massifIndex is used to identify the test case's desired results to the mock implementations above.
		massifIndex uint64
	}
	tests := []struct {
		name          string
		callOpts      []massifs.ReaderOption
		args          args
		wantErr       error
		wantErrPrefix string
	}{
		{name: "tamper after seal", args: args{tenantIdentity: tenantId3InconsistentLogUpdate, massifIndex: 0}, wantErr: mmr.ErrConsistencyCheck},
		{
			name:     "local seal inconsistent with remote log",
			callOpts: []massifs.ReaderOption{massifs.WithTrustedBaseState(fakeGoodState)}, args: args{tenantIdentity: tenantId4RemoteInconsistentWithTrustedSeal, massifIndex: 0},
			wantErr: mmr.ErrConsistencyCheck,
		},

		// provide an invalid public signing key, this simulates a remote log being signed by a different key than the verifier expects
		// {name: "invalid public seal key", args: args{tenantIdentity: tenantId3InconsistentLogUpdate, massifIndex: 0}, wantErr: massifs.ErrRemoteSealKeyMatchFailed},
		{
			name:     "valid public seal key",
			callOpts: []massifs.ReaderOption{massifs.WithTrustedSealerPub(&tc.Key.PublicKey)},
			args:     args{tenantIdentity: tenantId5TrustedPublicKeyMismatch, massifIndex: 0},
			wantErr:  nil,
		},

		{
			name:     "invalid public seal key",
			callOpts: []massifs.ReaderOption{massifs.WithTrustedSealerPub(&fakeECKey.PublicKey)},
			args:     args{tenantIdentity: tenantId5TrustedPublicKeyMismatch, massifIndex: 0},
			wantErr:  massifs.ErrRemoteSealKeyMatchFailed,
		},

		// see the GetSignedRoot mock above for the rational behind tampering only a peak
		{name: "seal peak tamper", args: args{tenantIdentity: tenantId2TamperedLogUpdate, massifIndex: 0}, wantErr: massifs.ErrSealVerifyFailed},
		{name: "seal shorter than massif", args: args{tenantIdentity: tenantId1SealBehindLog, massifIndex: 0}},
		{name: "happy path", args: args{tenantIdentity: tenantId0, massifIndex: 0}},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {

				reader, err := massifs.NewLocalReader(logger.Sugar, dc)
				assert.NoError(t, err)
				_, err = reader.GetVerifiedContext(
					t.Context(),
					tt.args.tenantIdentity,
					tt.args.massifIndex,
					append(tt.callOpts, massifs.WithSealGetter(&sg))...)

				if tt.wantErr == nil {
					assert.Nil(t, err, "unexpected error")
				} else if tt.wantErr != nil {
					assert.NotNil(t, err, "expected error got nil")
					if !errors.Is(err, tt.wantErr) {
						assert.ErrorIs(t, err, tt.wantErr)
					}
				} else if tt.wantErrPrefix != "" {
					assert.NotNil(t, err, "expected error got nil")
					assert.True(t, strings.HasPrefix(err.Error(), tt.wantErrPrefix))
				}
				if tt.wantErr == nil || tt.wantErrPrefix == "" {
					return
				}
			},
		)
	}

}
