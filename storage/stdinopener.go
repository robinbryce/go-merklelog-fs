package storage

import (
	"bufio"
	"bytes"
	"io"
	"os"

	"github.com/robinbryce/go-merklelog-fs/filecache"
)

type StdinOpener struct {
	data []byte
}

func NewStdinOpener() filecache.Opener {
	return &StdinOpener{}
}

func (o *StdinOpener) Open(string) (io.ReadCloser, error) {
	if len(o.data) > 0 {
		return io.NopCloser(bytes.NewReader(o.data)), nil
	}

	r := bufio.NewReader(os.Stdin)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	o.data = data
	return io.NopCloser(bytes.NewReader(o.data)), nil
}
