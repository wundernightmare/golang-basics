package testx_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/testx"
)

// What Go's resolver makes of each answer the server can give: the errors
// the DNS tests of the clients rely on.
func TestDNS(t *testing.T) {
	t.Parallel()

	const name = "billing.dns.test"

	server := testx.NewDNS(t)
	resolver := server.Resolver()

	_, err := resolver.LookupHost(t.Context(), name)

	var notFound *net.DNSError

	require.ErrorAs(t, err, &notFound)
	assert.True(t, notFound.IsNotFound, "an unknown name is NXDOMAIN")

	server.Set(name, "127.0.0.1", "127.0.0.2")

	addrs, err := resolver.LookupHost(t.Context(), name)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"127.0.0.1", "127.0.0.2"}, addrs)

	server.Set(name, "127.0.0.3")

	addrs, err = resolver.LookupHost(t.Context(), "Billing.DNS.test.")
	require.NoError(t, err)
	assert.Equal(t, []string{"127.0.0.3"}, addrs, "the record moved; nothing is cached")

	server.Fail()

	_, err = resolver.LookupHost(t.Context(), name)

	var failed *net.DNSError

	require.ErrorAs(t, err, &failed)
	assert.False(t, failed.IsNotFound, "SERVFAIL is not \"no such host\"")
	assert.True(t, failed.IsTemporary)

	server.Silence()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err = resolver.LookupHost(ctx, name)

	var silent *net.DNSError

	require.ErrorAs(t, err, &silent)
	assert.True(t, silent.IsTimeout)

	server.Answer()
	server.Remove(name)

	_, err = resolver.LookupHost(t.Context(), name)
	require.ErrorAs(t, err, &notFound)
	assert.True(t, notFound.IsNotFound)
	assert.Positive(t, server.Queries(name))
}
