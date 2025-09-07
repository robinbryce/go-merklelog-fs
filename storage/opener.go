package storage
import "io"

type Opener interface {
	Open(string) (io.ReadCloser, error)
}