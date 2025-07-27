package storage

import (
	"fmt"
	"path/filepath"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

type PrefixProvider struct {
	Options *Options
}

func forceTrailingSlash(path string) string {
	if len(path) > 0 && path[len(path)-1] != '/' {
		return path + "/"
	}
	return path
}

func (d PrefixProvider) Prefix(logID storage.LogID, otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:

		// If we have an explicit filename, the prefix is it's containing directory
		if d.Options.MassifFilename != "" {
			return forceTrailingSlash(filepath.Dir(d.Options.MassifFilename)), nil
		}
		return forceTrailingSlash(d.Options.Dir), nil

	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:

		if d.Options.SealFilename != "" {
			return forceTrailingSlash(filepath.Dir(d.Options.SealFilename)), nil
		}
		if d.Options.SealDir != "" {
			return forceTrailingSlash(d.Options.SealDir), nil
		}
		return forceTrailingSlash(d.Options.Dir), nil

	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// LogID from the storage path according to the datatrails massif storage schema.
// The storage path is expected to be in the format:
// */tenant/<tenant_uuid>/*
func (d PrefixProvider) LogID(storagePath string) (storage.LogID, error) {

	logID := datatrails.TenantID2LogID(storagePath)
	if logID != nil {
		return logID, nil
	}
	return nil, fmt.Errorf("could not identify log ID in path: %s", storagePath)
}