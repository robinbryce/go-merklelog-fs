package storage

import (
	"os"
	"path/filepath"

	"github.com/robinbryce/go-merklelog-fs/filecache"
)

// OsDirLister provides utilities to remove the os dependencies from the MassifReader
type OsDirLister struct{}

func NewDirLister() filecache.DirLister {
	return &OsDirLister{}
}

func (*OsDirLister) ListFiles(name string) ([]string, error) {
	dpath, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	result := []string{}
	entries, err := os.ReadDir(dpath)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		// if !entry.IsDir() && entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), massifs.V1MMRMassifExt){
		if !entry.IsDir() {
			result = append(result, filepath.Join(dpath, entry.Name()))
		}
	}
	return result, nil
}
