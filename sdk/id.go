package sdk

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// NewID creates an opaque process-independent identifier. Applications should
// treat the representation as an implementation detail.
func NewID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}
