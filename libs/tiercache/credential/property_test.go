package credential_test

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/wundernightmare/golang-basics/libs/tiercache/credential"
)

// The codec is the cache's wire format: whatever the three strings hold —
// empty, long, arbitrary bytes — decoding what was encoded gives them back,
// appending never disturbs what the buffer already held, and the result
// shares nothing with the buffer it was decoded from.
func TestCodecRoundTripsAnyValue(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		want := credential.Credential{
			ID:           rapid.String().Draw(t, "id"),
			CRMID:        rapid.String().Draw(t, "crm id"),
			KeyEncrypted: string(rapid.SliceOfN(rapid.Byte(), 0, 4096).Draw(t, "key")),
		}
		prefix := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "what the buffer held")

		encoded, err := credential.Codec{}.Append(append([]byte(nil), prefix...), want)
		if err != nil {
			t.Fatalf("append: %v", err)
		}

		if string(encoded[:len(prefix)]) != string(prefix) {
			t.Fatalf("Append rewrote the bytes before it")
		}

		payload := encoded[len(prefix):]

		got, err := credential.Codec{}.Decode(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		for i := range payload {
			payload[i] = 0xFF // the cache reuses the buffer
		}

		if got != want {
			t.Fatalf("round trip changed the value:\n got %s\nwant %s", describe(got), describe(want))
		}
	})
}

func describe(c credential.Credential) string {
	return fmt.Sprintf("{ID:%q CRMID:%q KeyEncrypted:%d bytes}", c.ID, c.CRMID, len(c.KeyEncrypted))
}
