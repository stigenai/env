// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"errors"
	"sync"
)

const (
	// liveWindow is how many bytes not yet streamed a live_output process can
	// write before its writes block.
	liveWindow = 4 << 20
	// liveKeep is how many bytes already streamed stay, so a client can resume
	// at an offset after a broken stream.
	liveKeep = 4 << 20
	// liveChunk caps one read, below gRPC's default 4 MiB receive limit.
	liveChunk = 1 << 20
)

// errDropped means the offset is older than the bytes a liveBuffer keeps.
var errDropped = errors.New("output at this offset was already dropped")

// liveBuffer holds a live_output stream in memory. It has no total byte cap:
// a writer blocks while liveWindow bytes are not yet read, and bytes read
// more than liveKeep ago are dropped.
type liveBuffer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte // bytes from offset base
	base    int64
	read    int64 // the highest offset handed to a reader
	noBlock bool  // set by a terminating signal: drop the oldest bytes instead of blocking
	onWrite func()
}

func newLiveBuffer(onWrite func()) *liveBuffer {
	b := &liveBuffer{onWrite: onWrite}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Write appends p. It blocks while the unread window is full.
func (b *liveBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		b.trim(b.read - liveKeep)
		end := b.base + int64(len(b.buf))
		if b.noBlock {
			b.buf = append(b.buf, p...)
			b.trim(end + int64(len(p)) - liveWindow - liveKeep)
			break
		}
		room := liveWindow - int(end-b.read)
		if room <= 0 {
			b.onWrite() // wake readers before waiting for them
			b.cond.Wait()
			continue
		}
		k := min(room, len(p))
		b.buf = append(b.buf, p[:k]...)
		p = p[k:]
	}
	b.onWrite()
	return n, nil
}

// trim drops the bytes before offset to.
func (b *liveBuffer) trim(to int64) {
	if to <= b.base {
		return
	}
	k := min(int(to-b.base), len(b.buf))
	b.buf = b.buf[k:]
	b.base += int64(k)
}

// Read returns up to liveChunk bytes from offset and the next offset, like
// ReadSpool: an offset past the end returns nothing and keeps the offset.
func (b *liveBuffer) Read(offset int64) ([]byte, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset < b.base {
		return nil, offset, errDropped
	}
	end := b.base + int64(len(b.buf))
	if offset >= end {
		return nil, offset, nil
	}
	data := append([]byte(nil), b.buf[offset-b.base:min(end-b.base, offset-b.base+liveChunk)]...)
	next := offset + int64(len(data))
	if next > b.read {
		b.read = next
		b.cond.Broadcast()
	}
	return data, next, nil
}

// unblock makes Write drop the oldest bytes instead of waiting, so a process
// that is ending is not held by a reader that never comes.
func (b *liveBuffer) unblock() {
	b.mu.Lock()
	b.noBlock = true
	b.cond.Broadcast()
	b.mu.Unlock()
}
