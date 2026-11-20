package integration_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/tracehubmmp/golang-basics/libs/secret"
	"github.com/tracehubmmp/golang-basics/libs/testx"
	"github.com/tracehubmmp/golang-basics/libs/testx/containers"
	"github.com/tracehubmmp/golang-basics/libs/valkey"
)

const valkey72Image = "valkey/valkey:7.2-alpine"

type topologyNode struct {
	addr    string
	gateway string
	port    string
	busPort string
}

func TestTiercacheValkey72Standalone(t *testing.T) {
	t.Parallel()

	node := topologyAnchor(t, "valkey-72-standalone", false)
	exerciseTiercacheTopology(t, "valkey://"+node.addr)
}

func TestTiercacheValkey72Cluster(t *testing.T) {
	t.Parallel()

	nodes := []topologyNode{topologyAnchor(t, "valkey-72-cluster-0", true)}
	for range 2 {
		nodes = append(nodes, topologyExtraNode(t, true))
	}

	clients := make([]valkeygo.Client, len(nodes))
	for i, node := range nodes {
		clients[i] = topologyAdminClient(t, node)
	}

	for i := 1; i < len(nodes); i++ {
		topologyCommand(t, clients[0], "CLUSTER", "MEET", nodes[i].gateway, nodes[i].port, nodes[i].busPort)
	}

	const slots = 16384
	for i, client := range clients {
		start := i * slots / len(clients)
		end := (i+1)*slots/len(clients) - 1
		topologyCommand(t, client, "CLUSTER", "ADDSLOTSRANGE", strconv.Itoa(start), strconv.Itoa(end))
	}

	require.Eventually(t, func() bool {
		for _, client := range clients {
			info, err := client.Do(t.Context(), client.B().ClusterInfo().Build()).ToString()
			if err != nil || !strings.Contains(info, "cluster_state:ok") || !strings.Contains(info, "cluster_known_nodes:3") {
				return false
			}
		}

		return true
	}, 30*time.Second, 100*time.Millisecond)

	exerciseTiercacheTopology(t, "valkey://"+nodes[0].addr)
}

func TestTiercacheValkey72Sentinel(t *testing.T) {
	t.Parallel()

	primary := topologyAnchor(t, "valkey-72-sentinel-primary", false)
	sentinelPort, err := freeTopologyPort()
	require.NoError(t, err)

	config := fmt.Sprintf("bind 127.0.0.1\nport %d\nprotected-mode no\nsentinel monitor primary %s %s 1\n", sentinelPort, primary.gateway, primary.port)
	sentinel, err := testcontainers.Run(t.Context(), valkey72Image,
		testcontainers.WithEntrypoint("valkey-sentinel"),
		testcontainers.WithCmdArgs("/etc/sentinel.conf"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: strings.NewReader(config), ContainerFilePath: "/etc/sentinel.conf", FileMode: 0o666,
		}),
		testcontainers.WithHostConfigModifier(topologyHostNetwork),
		testcontainers.WithWaitStrategy(wait.ForAll(wait.ForLog("Sentinel ID is")).WithStartupTimeoutDefault(60*time.Second)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(sentinel, testcontainers.StopTimeout(0)) })

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(sentinelPort))
	exerciseTiercacheTopology(t, "valkey://"+addr+"?master_set=primary")
}

func exerciseTiercacheTopology(t *testing.T, connectionURL string) {
	t.Helper()

	remote := func() *valkey.Cache {
		cache, err := valkey.New(valkey.Config{
			URL: secret.New(connectionURL), DialTimeout: 5 * time.Second, OpTimeout: 5 * time.Second,
			CompareAndSet: valkey.CompareAndSetLua,
		}, nil)
		require.NoError(t, err)
		t.Cleanup(cache.Close)

		return cache
	}

	writerRemote := remote()
	casKey := testx.Unique("replace")
	stored, err := writerRemote.SetNX(t.Context(), casKey, "old", time.Minute)
	require.NoError(t, err)
	require.True(t, stored)
	stored, err = writerRemote.Replace(t.Context(), casKey, "old", "new", time.Minute)
	require.NoError(t, err)
	require.True(t, stored)
	stored, err = writerRemote.Replace(t.Context(), casKey, "old", "stale", time.Minute)
	require.NoError(t, err)
	require.False(t, stored)

	src, key := newSource(), testx.Unique("key")
	src.set(key, 1)
	writer := newHarness(t, writerRemote, src, realTime("", time.Minute, time.Minute))
	reader := newHarness(t, remote(), src, realTime(writer.name, time.Minute, time.Minute))
	writer.subscribed(t)
	reader.subscribed(t)
	require.Equal(t, "v1", writer.version(t, key))
	require.Equal(t, "v1", reader.version(t, key))

	src.set(key, 2)
	require.NoError(t, writer.cache.Invalidate(t.Context(), key))
	require.Eventually(t, func() bool { return reader.version(t, key) == "v2" }, 5*time.Second, 20*time.Millisecond)
}

func topologyAnchor(t *testing.T, name string, cluster bool) topologyNode {
	t.Helper()

	return containers.Fixture(t, name, func(ctx context.Context) (topologyNode, testcontainers.Container, error) {
		return startTopologyNode(ctx, cluster)
	})
}

func topologyExtraNode(t *testing.T, cluster bool) topologyNode {
	t.Helper()

	node, ctr, err := startTopologyNode(t.Context(), cluster)
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr, testcontainers.StopTimeout(0)) })

	return node
}

func startTopologyNode(ctx context.Context, cluster bool) (topologyNode, testcontainers.Container, error) {
	port, err := freeTopologyPort()
	if err != nil {
		return topologyNode{}, nil, err
	}

	args := []string{
		"--bind", "127.0.0.1", "--port", strconv.Itoa(port),
		"--appendonly", "no", "--save", "", "--protected-mode", "no",
	}

	if cluster {
		busPort, err := freeTopologyPort()
		if err != nil {
			return topologyNode{}, nil, err
		}

		args = append(args,
			"--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf", "--cluster-node-timeout", "5000",
			"--cluster-port", strconv.Itoa(busPort),
			"--cluster-announce-ip", "127.0.0.1", "--cluster-announce-port", strconv.Itoa(port),
			"--cluster-announce-bus-port", strconv.Itoa(busPort),
		)
	}

	ctr, err := testcontainers.Run(ctx, valkey72Image,
		testcontainers.WithEntrypoint("valkey-server"),
		testcontainers.WithCmdArgs(args...),
		testcontainers.WithHostConfigModifier(topologyHostNetwork),
		testcontainers.WithWaitStrategy(wait.ForAll(wait.ForLog("Ready to accept connections")).WithStartupTimeoutDefault(60*time.Second)))
	if err != nil {
		return topologyNode{}, ctr, err
	}

	node := topologyNode{
		addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), gateway: "127.0.0.1", port: strconv.Itoa(port),
	}
	if cluster {
		node.busPort = args[len(args)-1]
	}

	return node, ctr, nil
}

func freeTopologyPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(port)
}

// Host networking gives the host, cluster gossip and Sentinel one address for
// a node without fixed ports or rewriting the addresses valkey-go discovers.
func topologyHostNetwork(hc *container.HostConfig) { hc.NetworkMode = "host" }

func topologyAdminClient(t *testing.T, node topologyNode) valkeygo.Client {
	t.Helper()

	client, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{node.addr}, ForceSingleClient: true, DisableRetry: true,
		Dialer: net.Dialer{Timeout: 5 * time.Second}, ConnWriteTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

func topologyCommand(t *testing.T, client valkeygo.Client, args ...string) valkeygo.ValkeyResult {
	t.Helper()

	result := client.Do(t.Context(), client.B().Arbitrary(args...).Build())
	require.NoError(t, result.Error())

	return result
}
