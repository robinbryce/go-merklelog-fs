package storage

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

type prefixProvider interface {
	Prefix(logID storage.LogID, otype storage.ObjectType) (string, error)
	LogID(storagePath string) (storage.LogID, error)
}

// NewPathProvider creates a new instance of StoragePaths with the given logID
// If the logID is nil, it must be set later using SelectLog.
func NewPathProvider(logID storage.LogID, p prefixProvider) *storage.StoragePaths {
	return &storage.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}

func NewPathProviderFromPath(storagePath string, p prefixProvider) *storage.StoragePaths {
	logID := datatrails.TenantID2LogID(storagePath)
	return &storage.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}

func NewPathProviderFromLogID(logID storage.LogID, p prefixProvider) *storage.StoragePaths {
	return &storage.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}
