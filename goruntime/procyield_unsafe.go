//go:build go1.6 || go1.7 || go1.8 || go1.9 || go1.10 || go1.11 || go1.12 || go1.13 || go1.14 || go1.15 || go1.16 || go1.17 || go1.18 || go1.19 || go1.20 || go1.21
// +build go1.6 go1.7 go1.8 go1.9 go1.10 go1.11 go1.12 go1.13 go1.14 go1.15 go1.16 go1.17 go1.18 go1.19 go1.20 go1.21

package goruntime

import (
	_ "unsafe" // for go:linkname
)

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)
