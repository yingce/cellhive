package ltx

// WALFrame is one SQLite WAL frame from the local capture reader.
type WALFrame struct {
	PageNo uint32
	DBSize uint32
	Data   []byte
}

// WALTransaction preserves one committed SQLite transaction boundary.
type WALTransaction struct {
	Frames []WALFrame
}
