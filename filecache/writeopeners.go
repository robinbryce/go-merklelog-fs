package filecache

import (
	"io"
	"os"
)

const (
	ReadWriteAllPermission = 0666
)

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
