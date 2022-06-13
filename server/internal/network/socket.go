package network

import (
	"net"
	"sync"
	"time"
)

// Outbound holds pending data for a socket.
type Outbound struct {
	P   []byte        // Primary write buffer
	S   []byte        // Secondary for use post flush
	Nb  net.Buffers   // net.Buffers for writev IO
	Sz  int32         // limit size per []byte, uses variable BufSize constants, start, min, max.
	Sws int32         // Number of short writes, used for dynamic resizing.
	Pb  int64         // Total pending/queued bytes.
	Pm  int32         // Total pending/queued messages.
	Fsp int32         // Flush signals that are pending per producer from readLoop'S pcd.
	Sg  *sync.Cond    // To signal writeLoop that there is data to flush.
	Wdl time.Duration // Snapshot of write deadline.
	Mp  int64         // Snapshot of max pending for client.
	Lft time.Duration // Last flush time for Write.
	Stc chan struct{} // Stall chan we create to slow down producers on overrun, e.g. fan-in.
}
