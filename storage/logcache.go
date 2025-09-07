package storage

import "github.com/datatrails/go-datatrails-merklelog/massifs"

type MassifStoragePaths struct {
	Data       string
	Checkpoint string
}

type LogCache struct {
	MassifPaths      map[uint32]*MassifStoragePaths
	MassifData       map[string][]byte
	CheckpointData   map[string][]byte
	Starts           map[string]*massifs.MassifStart
	Checkpoints      map[string]*massifs.Checkpoint
	FirstMassifIndex uint32
	HeadMassifIndex  uint32
	FirstSealIndex   uint32
	HeadSealIndex    uint32
}
