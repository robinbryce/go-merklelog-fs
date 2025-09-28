package storage

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
)

const (
	ReadWriteAllPermission = 0666
)

type WriteOpener interface {
	OpenCreate(path string) (io.WriteCloser, error)
	OpenWrite(path string) (io.WriteCloser, error)
}

type defaultWriteOpener struct {
	CreatePerms os.FileMode
}

type StdinOpener struct {
	data []byte
}

type ReadOpener struct{}

func (*ReadOpener) Open(name string) (io.ReadCloser, error) {
	fpath, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	return os.Open(fpath)
}

func NewFileOpener() Opener {
	return &ReadOpener{}
}

func NewStdinOpener() Opener {
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

func NewDefaultWriteOpener(createPerms os.FileMode) *defaultWriteOpener {
	return &defaultWriteOpener{
		CreatePerms: createPerms,
	}
}

// OpenCreate creates the file at the given path for writing.
// It fails and returns an error if the file already exists.
func (wo *defaultWriteOpener) OpenCreate(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, wo.CreatePerms)
}

// OpenWrite opens the file at the given path for writing, truncating it if it exists.
func (wo *defaultWriteOpener) OpenWrite(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, wo.CreatePerms)
}

/*
// OpenAppend ensures the named file exists and is writable. Writes are appended to any existing content.
func OpenAppend(name string) (io.WriteCloser, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, ReadWriteAllPermission)
}

// OpenCreate ensures the named file exists and is writable. Existing content is truncated (replaced entirely).
func OpenCreate(name string) (io.WriteCloser, error) {
	return os.Create(name)
}

func OpenExclusiveCreate(name string) (io.WriteCloser, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, ReadWriteAllPermission)
}

func WriteAndClose(w io.WriteCloser, data []byte) error {
	n, err := w.Write(data)
	if err != nil {
		w.Close() // ensure we close the writer even if write fails
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return w.Close()
}
*/
