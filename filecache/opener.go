package filecache

import "io"

type Opener interface {
	Open(string) (io.ReadCloser, error)
}
