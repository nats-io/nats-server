package server

import "path/filepath"

type WALReader interface {
	LoadMsg(index uint64, sm *StoreMsg) (*StoreMsg, error)
	State() StreamState
	FastState(*StreamState)
	Stop() error
}

type WALEntry struct {
	Term, PrevTerm, PrevIndex uint64
	Data                      []byte // could be a checksum
}

func NewWALReader(storeDir string) (WALReader, error) {
	fsCfg := FileStoreConfig{
		StoreDir:   storeDir,
		BlockSize:  uint64(defaultMediumBlockSize),
		AsyncFlush: false,
		SyncAlways: true}
	streamCfg := StreamConfig{
		Name:    filepath.Base(storeDir),
		Storage: FileStorage}
	return newFileStore(fsCfg, streamCfg)
}

func LoadEntry(reader WALReader, index uint64) (*WALEntry, error) {
	var smp StoreMsg
	sm, err := reader.LoadMsg(index, &smp)
	if err != nil {
		return nil, err
	}
	ae, err := decodeAppendEntry(sm.msg, nil, _EMPTY_)
	if err != nil {
		return nil, err
	}
	return &WALEntry{Term: ae.term, PrevTerm: ae.pterm, PrevIndex: ae.pindex, Data: ae.buf}, nil
}
