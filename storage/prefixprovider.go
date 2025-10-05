package storage

import (
	"fmt"
	"path/filepath"

	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/google/uuid"
)

const (
	LogIDPrefix                = "log"
	LogIDParsePrefix           = LogIDPrefix + "/"
	DatatrailsLogIDPrefix      = "tenant"
	DatatrailsLogIDParsePrefix = DatatrailsLogIDPrefix + "/"
	CheckpointsDirName         = "checkpoints"
	MassifsDirName             = "massifs"
)

func (s CachingStore) PrefixPath(otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:

		// Use the logid and stick to the 'tenant/UUID' organization established by datatrails.
		return filepath.Join(s.Opts.RootDir, LogIDPrefix, uuid.UUID(s.SelectedLogID).String(), MassifsDirName) + "/", nil

	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:

		// Otherwise the logid and stick to the 'tenant/UUID' organization established by datatrails.
		return filepath.Join(s.Opts.RootDir, LogIDPrefix, uuid.UUID(s.SelectedLogID).String(), CheckpointsDirName) + "/", nil

	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// StoragePath2LogID from the storage path according to the datatrails massif storage schema.
// The storage path is expected to be in the format:
// */tenant/<tenant_uuid>/*
func StoragePath2LogID(storagePath string) (storage.LogID, error) {

	// prioritize the neutral, but support both for now.
	logID := storage.ParsePrefixedLogID(LogIDParsePrefix, storagePath)
	if logID != nil {
		return logID, nil
	}
	logID = storage.ParsePrefixedLogID(DatatrailsLogIDParsePrefix, storagePath)
	if logID != nil {
		return logID, nil
	}

	return nil, fmt.Errorf("could not identify log ID in path: %s", storagePath)
}
