package storage

type MassifStoragePaths struct {
	Data       string
	Checkpoint string
}

type LogCache struct {
	MassifPaths      map[uint32]*MassifStoragePaths
	MassifData       map[string][]byte
	CheckpointData   map[string][]byte
	FirstMassifIndex uint32
	HeadMassifIndex  uint32
	FirstSealIndex   uint32
	HeadSealIndex    uint32
}
