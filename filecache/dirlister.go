package filecache

type DirLister interface {
	// ListFiles returns list of absolute paths
	// to files (not subdirectories) in a directory
	ListFiles(string) ([]string, error)
}
