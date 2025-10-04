package storage

import (
	"context"
)

func NewStore(
	ctx context.Context, opts Options,
) (*CachingStore, error) {

	cachingStore := CachingStore{}

	if err := cachingStore.Init(ctx, &opts); err != nil {
		return nil, err
	}
	return &cachingStore, nil
}
