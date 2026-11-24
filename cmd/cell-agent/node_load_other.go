//go:build !linux

package main

import "time"

func processUsage() (time.Duration, int64, bool) { return 0, 0, false }
