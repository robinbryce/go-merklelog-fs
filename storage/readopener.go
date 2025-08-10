package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/robinbryce/go-merklelog-fs/filecache"
)

type ReadOpener struct{}

func (*ReadOpener) Open(name string) (io.ReadCloser, error) {
	fpath, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return os.Open(fpath)
}

func NewFileOpener() filecache.Opener {
	return &ReadOpener{}
}
