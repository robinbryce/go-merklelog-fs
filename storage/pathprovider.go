package storage

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

// NewPathProvider creates a new instance of StoragePaths with the given logID
// If the logID is nil, it must be set later using SelectLog.
func NewPathProvider(logID storage.LogID) *storageschema.StoragePaths {
	p := &PrefixProvider{}
	return &storageschema.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}

func NewPathProviderFromPath(storagePath string) *storageschema.StoragePaths {
	logID := datatrails.TenantID2LogID(storagePath)
	p := &PrefixProvider{}
	return &storageschema.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}
