package credential_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/tracehubmmp/golang-basics/libs/tiercache/credential"
)

var samples = []credential.Credential{
	{},
	{ID: "0193a5b8-7c1e-7d3a-9f52-1b6c0e4a8d21"},
	{ID: "0193a5b8-7c1e-7d3a-9f52-1b6c0e4a8d21", CRMID: "crm-42", KeyEncrypted: "AES256GCM:9f8e7d6c5b4a=="},
	{CRMID: "только crm", KeyEncrypted: strings.Repeat("k", 70_000)},
	{KeyEncrypted: "\x00\xff binary-looking, still a string"},
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	for _, want := range samples {
		encoded, err := credential.Codec{}.Append([]byte("prefix"), want)
		require.NoError(t, err)
		require.Equal(t, "prefix", string(encoded[:6]), "Append appends")

		got, err := credential.Codec{}.Decode(encoded[6:])
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

// messageType is credential.proto, built in code: the reference
// implementation's view of the schema, without a protoc toolchain.
func messageType(t testing.TB) protoreflect.MessageDescriptor {
	t.Helper()

	field := func(name string, number int32) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:   new(name),
			Number: new(number),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    new("credential.proto"),
		Package: new("tiercache.credential.v1"),
		Syntax:  new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  new("Credential"),
			Field: []*descriptorpb.FieldDescriptorProto{field("id", 1), field("crm_id", 2), field("key_encrypted", 3)},
		}},
	}, nil)
	require.NoError(t, err)

	return file.Messages().Get(0)
}

// The hand-written codec and google.golang.org/protobuf agree on the bytes,
// in both directions: what one writes the other reads, and (the encoding
// being deterministic for this schema) they write the same.
func TestWireCompatibleWithTheReferenceImplementation(t *testing.T) {
	t.Parallel()

	desc := messageType(t)
	fields := desc.Fields()

	for _, want := range samples[:4] { // the last sample is not valid UTF-8, which proto3 strings must be
		ours, err := credential.Codec{}.Append(nil, want)
		require.NoError(t, err)

		msg := dynamicpb.NewMessage(desc)
		require.NoError(t, proto.Unmarshal(ours, msg), "the reference implementation reads ours")
		assert.Equal(t, want.ID, msg.Get(fields.ByName("id")).String())
		assert.Equal(t, want.CRMID, msg.Get(fields.ByName("crm_id")).String())
		assert.Equal(t, want.KeyEncrypted, msg.Get(fields.ByName("key_encrypted")).String())

		theirs, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		require.NoError(t, err)
		assert.Equal(t, string(theirs), string(ours), "same bytes")

		got, err := credential.Codec{}.Decode(theirs)
		require.NoError(t, err, "we read the reference implementation's")
		assert.Equal(t, want, got)
	}
}

func TestDecodeSkipsUnknownFieldsAndTakesTheLastValue(t *testing.T) {
	t.Parallel()

	var wire []byte

	wire = protowire.AppendString(protowire.AppendTag(wire, 1, protowire.BytesType), "first")
	wire = protowire.AppendVarint(protowire.AppendTag(wire, 9, protowire.VarintType), 7)       // a field a newer writer added
	wire = protowire.AppendString(protowire.AppendTag(wire, 10, protowire.BytesType), "extra") // (same)
	wire = protowire.AppendString(protowire.AppendTag(wire, 1, protowire.BytesType), "last")

	got, err := credential.Codec{}.Decode(wire)
	require.NoError(t, err)
	assert.Equal(t, credential.Credential{ID: "last"}, got)
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	valid, err := credential.Codec{}.Append(nil, samples[2])
	require.NoError(t, err)

	for name, wire := range map[string][]byte{
		"truncated value":       valid[:len(valid)-1],
		"truncated tag":         {0x80},
		"length past the end":   {0x0a, 0x05, 'a'},
		"a string as a varint":  protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 1),
		"truncated unknown":     protowire.AppendTag(nil, 9, protowire.Fixed64Type),
		"field number zero":     {0x02, 0x00},
		"an envelope, not ours": []byte("\x00valkey:tombstone"),
	} {
		_, err := credential.Codec{}.Decode(wire)
		assert.Error(t, err, name)
	}
}

// Whatever the bytes: no panic, and what decodes survives a round trip.
func FuzzDecode(f *testing.F) {
	for _, sample := range samples {
		wire, _ := credential.Codec{}.Append(nil, sample)
		f.Add(wire)
	}

	f.Add([]byte{0x0a, 0x05, 'a'})

	f.Fuzz(func(t *testing.T, wire []byte) {
		decoded, err := credential.Codec{}.Decode(wire)
		if err != nil {
			return
		}

		again, err := credential.Codec{}.Append(nil, decoded)
		require.NoError(t, err)

		back, err := credential.Codec{}.Decode(again)
		require.NoError(t, err)
		require.Equal(t, decoded, back)
	})
}

//nolint:paralleltest // serial: testing.AllocsPerRun refuses to run in a parallel test
func TestAppendDoesNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 256)
	allocs := testing.AllocsPerRun(1000, func() {
		buf, _ = credential.Codec{}.Append(buf[:0], samples[2])
	})
	assert.Zero(t, allocs)
}
