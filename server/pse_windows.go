// Copyright 2015 Apcera Inc. All rights reserved.

package server

import ()

// This is a placeholder for now.
func procUsage(pcpu *float64, rss, vss *int64) error {
	*pcpu = 0.0
	*rss = 0
	*vss = 0

	return nil
}
