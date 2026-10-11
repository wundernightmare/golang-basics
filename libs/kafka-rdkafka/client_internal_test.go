package rdkafka

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/kafka"
)

// No broker needed: librdkafka builds clients without connecting, and a
// record to an unreachable broker just waits in the local queue.
func offline(t *testing.T) kafka.Config {
	t.Helper()
	cfg := kafka.Config{Brokers: []string{"127.0.0.1:1"}, Topics: []string{"t"}, Group: "g",
		PublishTimeout: 300 * time.Millisecond}
	require.NoError(t, cfg.Validate())
	return cfg
}

// A fatal error (lost idempotent producer state) replaces the client; the
// producer keeps working on the new one.
func TestProducer_FatalErrorReplacesClient(t *testing.T) {
	p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
	require.NoError(t, err)
	defer p.Close()

	failed := p.current()
	require.Equal(t, ckafka.ErrNoError, failed.TestFatalError(ckafka.ErrOutOfOrderSequenceNumber, "test"))
	require.Eventually(t, func() bool { return p.current() != failed }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, failed.IsClosed, 10*time.Second, 10*time.Millisecond, "the failed client is closed once replaced")

	replaced := p.current()
	p.wg.Add(1)
	p.replace(failed) // a late second report about the same client is a no-op
	assert.Same(t, replaced, p.current())
}

// current is the client in use.
func (p *producer) current() *ckafka.Producer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cl
}

// trackClients records every client a producer creates from now on.
func trackClients(t *testing.T) func() []*ckafka.Producer {
	t.Helper()
	var mu sync.Mutex
	var created []*ckafka.Producer
	testHookNewClient = func(cl *ckafka.Producer) { mu.Lock(); created = append(created, cl); mu.Unlock() }
	t.Cleanup(func() { testHookNewClient = nil })
	return func() []*ckafka.Producer { mu.Lock(); defer mu.Unlock(); return slices.Clone(created) }
}

// A fatal report that arrives once Close has begun creates no client: one
// made then would never be closed (its threads keep reconnecting) — and the
// old code also raced Close's WaitGroup.Wait with the new client's Add.
func TestProducer_NoReplacementAfterClose(t *testing.T) {
	created := trackClients(t)
	p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
	require.NoError(t, err)
	failed := p.current()
	p.Close()

	p.startReplace(failed)
	p.wg.Wait()
	require.Len(t, created(), 1, "no client after Close")
	assert.True(t, failed.IsClosed())
}

// Replacements racing sends and Close (run with -race, repeatedly): every
// client ever created ends closed, no send touches a destroyed one, and
// Close waits for a replacement under way.
func TestProducer_ReplaceRacesSendAndClose(t *testing.T) {
	for range 10 {
		created := trackClients(t)
		p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for ctx.Err() == nil {
					sctx, scancel := context.WithTimeout(ctx, 5*time.Millisecond)
					_ = p.Send(sctx, kafka.Message{Topic: "t", Value: []byte("x")})
					_ = p.Ping(sctx)
					scancel()
				}
			})
		}
		for range 3 {
			p.mu.RLock() // like any call into the client: never into one being closed
			p.cl.TestFatalError(ckafka.ErrOutOfOrderSequenceNumber, "test")
			p.mu.RUnlock()
			time.Sleep(time.Millisecond)
		}
		p.Close()
		cancel()
		wg.Wait()

		for _, cl := range created() {
			assert.True(t, cl.IsClosed(), "every client the producer created is closed")
		}
		var se *kafka.SendError
		require.ErrorAs(t, p.Send(context.Background(), kafka.Message{Topic: "t"}), &se, "a send after Close fails cleanly")
		require.Error(t, p.Ping(context.Background()))
	}
}

// Close never destroys the consumer under a call still inside librdkafka (a
// commit or a readiness ping whose caller gave up on its ctx): it waits for
// every registered call, refuses new pings meanwhile, and only then closes.
func TestConsumer_CloseWaitsForCallsInside(t *testing.T) {
	cl, err := Driver{}.NewConsumer(offline(t), slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.NoError(t, err)
	c := cl.(*consumer)

	c.calls.Add(1) // a call inside librdkafka
	closed := make(chan struct{})
	go func() { cl.Close(); close(closed) }()
	require.Eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.closing }, 5*time.Second, time.Millisecond)
	require.ErrorIs(t, cl.Ping(context.Background()), kafka.ErrClientClosed, "no new call while closing")
	select {
	case <-closed:
		t.Fatal("Close returned while a call was inside the client")
	case <-time.After(300 * time.Millisecond):
	}
	assert.False(t, c.cl.IsClosed())

	c.calls.Done()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish once the call returned")
	}
	assert.True(t, c.cl.IsClosed())
	require.ErrorIs(t, cl.Commit(context.Background(), []kafka.Offset{{TopicPartition: kafka.TopicPartition{Topic: "t"}, Next: 1}}), kafka.ErrClientClosed)
	_, err = cl.Poll(context.Background(), 1)
	require.ErrorIs(t, err, kafka.ErrClientClosed)
}

// A call that never returns does not hang shutdown: past closeWait Close
// gives up — and leaves the client alone rather than destroy it under the
// call (which would crash the process).
func TestConsumer_CloseGivesUpOnAStuckCall(t *testing.T) {
	old := closeWait
	closeWait = 200 * time.Millisecond
	t.Cleanup(func() { closeWait = old })
	var buf safeBuffer
	cl, err := Driver{}.NewConsumer(offline(t), slog.New(slog.NewJSONHandler(&buf, nil)), kafka.Rebalance{})
	require.NoError(t, err)
	c := cl.(*consumer)

	c.calls.Add(1)
	start := time.Now()
	cl.Close()
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.False(t, c.cl.IsClosed(), "not destroyed under the stuck call")
	assert.Contains(t, buf.String(), "did not return")
	require.ErrorIs(t, cl.Commit(context.Background(), nil), kafka.ErrClientClosed)

	c.calls.Done()
	_ = c.cl.Close() // the test's own cleanup of the abandoned handle
}

type safeBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *safeBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return string(s.b) }

// Send gives up with a retryable error when ctx ends before the delivery
// report; a fatal librdkafka error is KindFatal.
func TestProducer_SendBoundedByContext(t *testing.T) {
	p, err := newProducer(offline(t), slog.New(slog.DiscardHandler), "producer")
	require.NoError(t, err)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = p.Send(ctx, kafka.Message{Topic: "t", Value: []byte("x"), Headers: []kafka.Header{{Key: "h", Value: []byte("v")}}})
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	assert.True(t, se.Retryable)

	require.ErrorAs(t, sendError(ckafka.NewError(ckafka.ErrFatal, "fenced", true)), &se)
	assert.Equal(t, kafka.KindFatal, se.Kind)
}

// The dead-letter producer is built on first use; one that cannot be built
// fails every send with that reason instead of panicking the loop.
func TestConsumer_DeadLetterProducerThatCannotBeBuilt(t *testing.T) {
	cfg := offline(t)
	cfg.ProducerExtraProperties = kafka.Properties{"no.such.property": "1"} // producers only: the consumer still builds
	cl, err := Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.NoError(t, err)
	defer cl.Close()

	cl.AllowRebalance() // librdkafka rebalances inside Poll: nothing to release
	dlq := cl.DeadLetter()
	err = dlq.Send(context.Background(), kafka.Message{Topic: "t.dlq"})
	var se *kafka.SendError
	require.ErrorAs(t, err, &se)
	require.ErrorContains(t, err, "no.such.property")
	require.Error(t, dlq.Ping(context.Background()))
	assert.False(t, dlq.Stats().Valid)
	dlq.Close()
}

func TestNewConsumer_Errors(t *testing.T) {
	cfg := offline(t)
	cfg.ConsumerExtraProperties = kafka.Properties{"no.such.property": "1"}
	_, err := Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.ErrorContains(t, err, "no.such.property")

	cfg = offline(t)
	cfg.SASLPasswordFile = "/nonexistent/password"
	cfg.SecurityProtocol, cfg.SASLMechanism, cfg.SASLUsername = "sasl_plaintext", "SCRAM-SHA-512", "u"
	_, err = Driver{}.NewConsumer(cfg, slog.New(slog.DiscardHandler), kafka.Rebalance{})
	require.Error(t, err)
	_, err = Driver{}.NewProducer(cfg, slog.New(slog.DiscardHandler))
	require.Error(t, err)
}

// Repeats within the fold window are counted, not logged; the next line
// after the window carries the count. Unparseable statistics are ignored.
func TestPumpLogs_FoldsRepeats(t *testing.T) {
	var buf bytes.Buffer
	logs := make(chan ckafka.LogEvent, 4)
	ev := ckafka.LogEvent{Level: 3, Tag: "FAIL", Name: "rdkafka#producer-1", Message: "Connect to 127.0.0.1:1 failed"}
	logs <- ev
	logs <- ev
	logs <- ev
	close(logs)
	pumpLogs(logs, slog.New(slog.NewJSONHandler(&buf, nil)), "producer")
	assert.Equal(t, 1, strings.Count(buf.String(), "Connect to"), buf.String())
	assert.Contains(t, buf.String(), `"level":"WARN"`)

	h := &statsHolder{}
	h.update("not json")
	assert.False(t, h.clientStats(kafka.Config{}).Valid)
}
