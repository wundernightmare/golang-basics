package franz

import (
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// stats implements the franz-go hooks behind kafka.ClientStats: live
// connections per broker and request errors, plus the lag snapshot the
// consumer polls with kadm.
type stats struct {
	cfg kafka.Config
	cl  *kgo.Client

	mu        sync.Mutex
	conns     map[int32]int // open connections per broker node id
	reqErrors int64
	lagMax    int64
	lagSum    int64
	lagAt     time.Time
}

func newStats(cfg kafka.Config) *stats { return &stats{cfg: cfg, conns: map[int32]int{}} }

var (
	_ kgo.HookBrokerConnect    = (*stats)(nil)
	_ kgo.HookBrokerDisconnect = (*stats)(nil)
	_ kgo.HookBrokerWrite      = (*stats)(nil)
	_ kgo.HookBrokerRead       = (*stats)(nil)
)

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (s *stats) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		s.addError()
		return
	}
	s.mu.Lock()
	s.conns[meta.NodeID]++
	s.mu.Unlock()
}

// OnBrokerDisconnect implements kgo.HookBrokerDisconnect.
func (s *stats) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	s.mu.Lock()
	if s.conns[meta.NodeID]--; s.conns[meta.NodeID] <= 0 {
		delete(s.conns, meta.NodeID)
	}
	s.mu.Unlock()
}

// OnBrokerWrite implements kgo.HookBrokerWrite.
func (s *stats) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		s.addError()
	}
}

// OnBrokerRead implements kgo.HookBrokerRead.
func (s *stats) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		s.addError()
	}
}

func (s *stats) addError() {
	s.mu.Lock()
	s.reqErrors++
	s.mu.Unlock()
}

func (s *stats) setLag(lagMax, lagSum int64) {
	s.mu.Lock()
	s.lagMax, s.lagSum, s.lagAt = lagMax, lagSum, time.Now()
	s.mu.Unlock()
}

// common is the connection part; seed brokers (negative node ids) are not
// brokers of the cluster.
func (s *stats) common() kafka.ClientStats {
	up := int64(0)
	for id := range s.conns {
		if id >= 0 {
			up++
		}
	}
	return kafka.ClientStats{Valid: s.cfg.StatisticsInterval > 0, BrokersUp: up, RequestErrors: s.reqErrors}
}

func (s *stats) producer() kafka.ClientStats {
	s.mu.Lock()
	st := s.common()
	s.mu.Unlock()
	if s.cl != nil {
		st.QueueBytes = s.cl.BufferedProduceBytes()
	}
	st.QueueMaxBytes = int64(s.cfg.ProducerQueueMaxKBytes) * 1024
	return st
}

// consumer reports only while the lag snapshot is fresh: a client that
// stopped polling must not freeze its last numbers on a dashboard.
func (s *stats) consumer() kafka.ClientStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.common()
	maxAge := max(3*time.Duration(s.cfg.StatisticsInterval)*time.Millisecond, time.Minute)
	if s.lagAt.IsZero() || time.Since(s.lagAt) > maxAge {
		st.Valid = false
	}
	st.LagMax, st.LagSum = s.lagMax, s.lagSum
	return st
}
