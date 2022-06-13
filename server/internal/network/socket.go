package network

import (
	"net"
	"sync"
	"time"
)

// Outbound holds pending data for a socket.
type Outbound struct {
	P   []byte        // Primary write buffer
	s   []byte        // Secondary for use post flush
	Nb  net.Buffers   // net.Buffers for writev IO
	sz  int32         // limit size per []byte, uses variable BufSize constants, start, min, max.
	sws int32         // Number of short writes, used for dynamic resizing.
	Pb  int64         // Total pending/queued bytes.
	pm  int32         // Total pending/queued messages.
	fsp int32         // Flush signals that are pending per producer from readLoop's pcd.
	Sg  *sync.Cond    // To signal writeLoop that there is data to flush.
	wdl time.Duration // Snapshot of write deadline.
	mp  int64         // Snapshot of max pending for client.
	lft time.Duration // Last flush time for Write.
	stc chan struct{} // Stall chan we create to slow down producers on overrun, e.g. fan-in.
}
