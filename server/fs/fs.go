package fs

import (
	"embed"
)

//go:embed errors.json

// FS is an embedded filesystem holding various support data
var FS embed.FS
