package storage

import (
	"fmt"
	"path/filepath"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
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

type PrefixProvider struct {
	Dir string // The directory where the massif files are stored
}

func (d PrefixProvider) Prefix(logID storage.LogID, otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:

		// Use the logid and stick to the 'tenant/UUID' organization established by datatrails.
		return filepath.Join(d.Dir, LogIDPrefix, uuid.UUID(logID).String(), MassifsDirName) + "/", nil

	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:

		// Otherwise the logid and stick to the 'tenant/UUID' organization established by datatrails.
		return filepath.Join(d.Dir, LogIDPrefix, uuid.UUID(logID).String(), CheckpointsDirName) + "/", nil

	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// LogID from the storage path according to the datatrails massif storage schema.
// The storage path is expected to be in the format:
// */tenant/<tenant_uuid>/*
func (d PrefixProvider) LogID(storagePath string) (storage.LogID, error) {

	// prioritize the neutral, but support both for now.
	logID := storageschema.ParsePrefixedLogID(LogIDParsePrefix, storagePath)
	if logID != nil {
		return logID, nil
	}
	logID = storageschema.ParsePrefixedLogID(DatatrailsLogIDParsePrefix, storagePath)
	if logID != nil {
		return logID, nil
	}

	return nil, fmt.Errorf("could not identify log ID in path: %s", storagePath)
}
