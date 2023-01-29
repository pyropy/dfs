package model

import (
	"github.com/google/uuid"
	"time"
)

type Lease struct {
	ChunkID       uuid.UUID
	ValidUntil    time.Time
	ChunkServerID uuid.UUID
}

func (l *Lease) IsExpired() bool {
	now := time.Now()

	return !l.ValidUntil.After(now)
}
