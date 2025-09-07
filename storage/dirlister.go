package storage

import "strings"

type SuffixDirLister struct {
	OsDirLister
	Suffix string
}

func NewSuffixDirLister(suffix string) DirLister {
	return &SuffixDirLister{Suffix: suffix}
}

func (s *SuffixDirLister) ListFiles(name string) ([]string, error) {
	found, err := s.OsDirLister.ListFiles(name)
	if err != nil {
		return nil, err
	}
	var matched []string
	for _, f := range found {
		if strings.HasSuffix(f, s.Suffix) {
			matched = append(matched, f)
		}
	}
	return matched, nil
}
