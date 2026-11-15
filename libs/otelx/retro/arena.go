package retro

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// The memory of the package: encoded spans in byte rings of a fixed size.
//
// A shard is one ring of bytes, allocated once at its full size and never
// grown. A span is appended as a record — a small header and the bytes of
// the codec — and when the ring has no room the oldest records leave.
// Nothing else is kept per span, so the size configured is the memory used,
// and it is memory without pointers: the garbage collector never scans it.
//
// A trace lives in one shard (by a hash of its id, seeded per process — an
// id that arrives over HTTP is the client's choice), so ending a span and
// triggering a trace each take one lock, and a trigger walks one ring.
const (
	// shardsMax is the most shards an arena has.
	shardsMax = 64
	// shardMinBytes is the least a shard holds, so a small buffer has fewer
	// shards: a span is a few hundred bytes.
	shardMinBytes = 256 << 10
	// triggeredMax is how many triggered trace ids are remembered over all
	// shards, so that the spans of a triggered trace that end later are
	// exported too. The oldest leaves first.
	triggeredMax = 4096

	// A record header: payload length, state, trace id, end time of the span.
	headerLen = 4 + 1 + 16 + 8
	stateAt   = 4

	stateLive byte = 0
	// stateTaken: the span was handed to a trigger; the record keeps its
	// place until it reaches the front, so a trigger never moves the ring.
	stateTaken byte = 1
)

type header struct {
	payload  int
	state    byte
	traceID  trace.TraceID
	endNanos int64
}

// shard is one ring and the triggered traces that hash to it.
type shard struct {
	mu sync.Mutex
	// buf is the ring: records, oldest first, from head, size bytes of them.
	// Allocated once and never grown.
	buf        []byte
	head, size int
	// spans counts the live records.
	spans int
	// newestNanos is the end time of the newest record ever pushed.
	newestNanos int64

	// triggered is the set of trace ids exported from now on; order is the
	// same ids, oldest first, in a ring of its own.
	triggered map[trace.TraceID]struct{}
	order     []trace.TraceID
	orderHead int

	// What happened in this shard, counted under its lock: a counter every
	// goroutine bumps for every span is a cache line they all wait for.
	kept, evicted, taken        uint64
	accepted, repeated, refused uint64
}

func newShard(capacity, triggered int) *shard {
	return &shard{
		buf:       make([]byte, capacity),
		triggered: make(map[trace.TraceID]struct{}, triggered),
		order:     make([]trace.TraceID, 0, triggered),
	}
}

// at maps an offset from the oldest byte to an index of buf.
func (s *shard) at(offset int) int {
	i := s.head + offset
	if i >= len(s.buf) {
		i -= len(s.buf)
	}

	return i
}

// read copies len(dst) bytes that start offset bytes after the oldest one.
func (s *shard) read(offset int, dst []byte) {
	i := s.at(offset)
	n := copy(dst, s.buf[i:])
	copy(dst[n:], s.buf)
}

// append writes p after the newest byte. The caller made the room.
func (s *shard) append(p []byte) {
	i := s.at(s.size)
	n := copy(s.buf[i:], p)
	copy(s.buf, p[n:])
	s.size += len(p)
}

// header reads the header of the record offset bytes after the oldest byte;
// false when there is no whole record there.
func (s *shard) header(offset int) (header, bool) {
	if offset+headerLen > s.size {
		return header{}, false
	}

	var raw [headerLen]byte

	s.read(offset, raw[:])

	hdr := header{
		payload:  int(binary.LittleEndian.Uint32(raw[:4])),
		state:    raw[stateAt],
		endNanos: int64(binary.LittleEndian.Uint64(raw[21:])), //nolint:gosec // G115: written from an int64 by push
	}
	copy(hdr.traceID[:], raw[5:21])

	if offset+headerLen+hdr.payload > s.size {
		return header{}, false
	}

	return hdr, true
}

// pop drops the oldest record; false when the ring holds none.
func (s *shard) pop() bool {
	hdr, ok := s.header(0)
	if !ok {
		// Not a whole record at the front: the ring is not ours any more.
		// Start it over rather than misread it.
		s.head, s.size, s.spans = 0, 0, 0

		return false
	}

	s.head = s.at(headerLen + hdr.payload)
	s.size -= headerLen + hdr.payload

	if hdr.state == stateLive {
		s.spans--
		s.evicted++
	}

	return true
}

// push appends a record, evicting the oldest ones for it. A payload larger
// than the shard is not kept and counts as one evicted span.
func (s *shard) push(traceID trace.TraceID, endNanos int64, payload []byte) {
	record := headerLen + len(payload)
	if record > len(s.buf) {
		s.evicted++

		return
	}

	for s.size+record > len(s.buf) && s.pop() {
	}

	var raw [headerLen]byte

	binary.LittleEndian.PutUint32(raw[:4], uint32(len(payload))) //nolint:gosec // G115: bounded by the shard, far below 4 GiB
	raw[stateAt] = stateLive
	copy(raw[5:21], traceID[:])
	binary.LittleEndian.PutUint64(raw[21:], uint64(endNanos)) //nolint:gosec // G115: the bit pattern is read back as int64

	s.append(raw[:])
	s.append(payload)

	s.spans++
	s.kept++
	s.newestNanos = max(s.newestNanos, endNanos)
}

// take hands out copies of the payloads of the live records of traceID,
// oldest first, and marks those records taken.
func (s *shard) take(traceID trace.TraceID) [][]byte {
	var out [][]byte

	for offset := 0; ; {
		hdr, ok := s.header(offset)
		if !ok {
			break
		}

		if hdr.state == stateLive && hdr.traceID == traceID {
			payload := make([]byte, hdr.payload)
			s.read(offset+headerLen, payload)
			out = append(out, payload)

			s.buf[s.at(offset+stateAt)] = stateTaken
			s.spans--
			s.taken++
		}

		offset += headerLen + hdr.payload
	}

	// Taken records at the front free their room now.
	for {
		hdr, ok := s.header(0)
		if !ok || hdr.state != stateTaken {
			break
		}

		s.pop()
	}

	return out
}

// horizon is the time between the oldest and the newest span kept.
func (s *shard) horizon() time.Duration {
	for offset := 0; ; {
		hdr, ok := s.header(offset)
		if !ok {
			return 0
		}

		if hdr.state == stateLive {
			return time.Duration(max(s.newestNanos-hdr.endNanos, 0))
		}

		offset += headerLen + hdr.payload
	}
}

// remember adds traceID to the triggered set, forgetting the oldest id when
// the set is full; whether one was forgotten.
func (s *shard) remember(traceID trace.TraceID) (forgot bool) {
	if len(s.order) == cap(s.order) {
		oldest := s.order[s.orderHead]
		delete(s.triggered, oldest)
		s.order[s.orderHead] = traceID
		s.orderHead = (s.orderHead + 1) % len(s.order)
		forgot = true
	} else {
		s.order = append(s.order, traceID)
	}

	s.triggered[traceID] = struct{}{}

	return forgot
}

// Outcome is what a trigger did.
type Outcome int

const (
	// Accepted means the trace is exported from now on.
	Accepted Outcome = iota
	// Repeated means the trace was triggered before.
	Repeated
	// RateLimited means too many triggers this second: the trace stays
	// unsampled.
	RateLimited
)

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Repeated:
		return "repeated"
	case RateLimited:
		return "rate_limited"
	default:
		return "unknown"
	}
}

// usage is a snapshot of the arena.
type usage struct {
	spans   int           // spans kept
	bytes   int           // bytes of the rings in use, headers and taken records included
	horizon time.Duration // between the oldest and the newest span kept

	kept, evicted, taken        uint64
	accepted, repeated, refused uint64
}

// Bits of arena.window that count the triggers of the second; what the bits
// above them hold is said at the field.
const (
	windowCountBits = 24
	windowCountMask = 1<<windowCountBits - 1
)

// arena is byte rings in shards by trace id, and the traces that were
// triggered.
type arena struct {
	shards []*shard
	seed   maphash.Seed
	// window is the rate limit of the triggers, in one word and without a
	// lock: under a storm of errors every warn line of every goroutine asks
	// it. The millisecond the counted second began at (plus one, zero = none
	// yet) and the triggers admitted in it.
	window  atomic.Uint64
	created time.Time
	// perSecond is the traces triggered a second at most.
	perSecond uint64
	// triggeredCount is the triggered traces remembered over all shards: the
	// check of every span and log line costs one atomic load while no trace
	// is triggered.
	triggeredCount atomic.Int64
}

// newArena builds an arena of capacity bytes in as many shards as keep each
// of them at least shardMinBytes large.
func newArena(capacity, perSecond int, now time.Time) *arena {
	shards := min(max(capacity/shardMinBytes, 1), shardsMax)
	// Round down to a power of two.
	for shards&(shards-1) != 0 {
		shards &= shards - 1
	}

	return newArenaWithShards(capacity, shards, perSecond, now)
}

func newArenaWithShards(capacity, shards, perSecond int, now time.Time) *arena {
	shards = max(shards, 1)
	a := &arena{
		shards:    make([]*shard, shards),
		seed:      maphash.MakeSeed(),
		created:   now,
		perSecond: uint64(max(perSecond, 0)),
	}

	for i := range a.shards {
		a.shards[i] = newShard(max(capacity, 0)/shards, max(triggeredMax/shards, 1))
	}

	return a
}

func (a *arena) shard(traceID trace.TraceID) *shard {
	return a.shards[maphash.Bytes(a.seed, traceID[:])%uint64(len(a.shards))]
}

// isTriggered reports whether the spans of traceID are exported.
func (a *arena) isTriggered(traceID trace.TraceID) bool {
	if a.triggeredCount.Load() == 0 {
		return false
	}

	s := a.shard(traceID)

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.triggered[traceID]

	return ok
}

// offer keeps the encoded span payload of traceID, unless the trace is
// triggered — then nothing is kept and the caller exports the span.
func (a *arena) offer(traceID trace.TraceID, endNanos int64, payload []byte) (triggered bool) {
	s := a.shard(traceID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.triggered[traceID]; ok {
		return true
	}

	s.push(traceID, endNanos, payload)

	return false
}

// trigger triggers traceID at now: the outcome and the encoded spans of the
// trace that the arena kept, oldest first.
func (a *arena) trigger(traceID trace.TraceID, now time.Time) (Outcome, [][]byte) {
	s := a.shard(traceID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.triggered[traceID]; ok {
		s.repeated++

		return Repeated, nil
	}

	if !a.admit(now) {
		s.refused++

		return RateLimited, nil
	}

	if !s.remember(traceID) {
		a.triggeredCount.Add(1)
	}

	s.accepted++

	return Accepted, s.take(traceID)
}

// admit is the rate limit: whether one more trace may be triggered at now.
func (a *arena) admit(now time.Time) bool {
	limit := min(a.perSecond, windowCountMask)
	if limit == 0 {
		return false
	}

	nowMS := uint64(max(now.Sub(a.created).Milliseconds(), 0))

	for {
		seen := a.window.Load()
		began, count := seen>>windowCountBits, seen&windowCountMask
		// began is one past the millisecond, so that zero means "none": the
		// window is the thousand milliseconds from began-1. A caller whose
		// clock reading is older than that raced the one that opened it,
		// and the window is open for it too.
		open := began != 0 && nowMS+1 < began+1000

		var next uint64

		switch {
		case !open:
			next = (nowMS+1)<<windowCountBits | 1
		case count >= limit:
			return false
		default:
			next = seen + 1
		}

		if a.window.CompareAndSwap(seen, next) {
			return true
		}
	}
}

// usage is what the arena holds now: exact, locks every shard in turn. The
// horizon is the shortest one among the shards that hold spans.
func (a *arena) usage() usage {
	var (
		out      usage
		shortest = time.Duration(math.MaxInt64)
	)

	for _, s := range a.shards {
		if horizon, holds := s.add(&out); holds {
			shortest = min(shortest, horizon)
		}
	}

	if out.spans > 0 {
		out.horizon = shortest
	}

	return out
}

// add adds what the shard holds and counted to out, and returns its horizon
// when it holds spans.
func (s *shard) add(out *usage) (horizon time.Duration, holds bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out.spans += s.spans
	out.bytes += s.size
	out.kept += s.kept
	out.evicted += s.evicted
	out.taken += s.taken
	out.accepted += s.accepted
	out.repeated += s.repeated
	out.refused += s.refused

	if s.spans == 0 {
		return 0, false
	}

	return s.horizon(), true
}

// allocated is the bytes the rings were allocated with: the memory of the
// arena.
func (a *arena) allocated() int {
	total := 0
	for _, s := range a.shards {
		total += len(s.buf)
	}

	return total
}
