// Package credential is the worked example of a tiercache value: a small
// record of string identifiers stored as protobuf (credential.proto).
//
// The codec is hand-written on protowire — the wire-level half of
// google.golang.org/protobuf — rather than generated: three string fields
// need no reflection, Append writes straight into the cache's buffer without
// allocating, and Decode allocates exactly the strings it returns. A larger
// or evolving schema is better served by generated code behind the same
// tiercache.Codec interface.
package credential

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Credential is the cached record. KeyEncrypted is ciphertext: the cache
// never sees the key in the clear, and nothing here logs a value.
type Credential struct {
	ID           string
	CRMID        string
	KeyEncrypted string
}

// Field numbers of credential.proto.
const (
	fieldID           protowire.Number = 1
	fieldCRMID        protowire.Number = 2
	fieldKeyEncrypted protowire.Number = 3
)

// Codec encodes [Credential] as the protobuf message of credential.proto. It
// implements tiercache.Codec[Credential].
type Codec struct{}

// Append appends the encoded message to dst. Empty fields are omitted, as
// proto3 does.
func (Codec) Append(dst []byte, value Credential) ([]byte, error) {
	dst = appendString(dst, fieldID, value.ID)
	dst = appendString(dst, fieldCRMID, value.CRMID)
	dst = appendString(dst, fieldKeyEncrypted, value.KeyEncrypted)

	return dst, nil
}

func appendString(dst []byte, num protowire.Number, value string) []byte {
	if value == "" {
		return dst
	}

	return protowire.AppendString(protowire.AppendTag(dst, num, protowire.BytesType), value)
}

// Decode parses a message. Unknown fields are skipped (a newer writer may
// add some), the last occurrence of a field wins, and malformed input is an
// error, never a panic. The result does not alias src.
func (Codec) Decode(src []byte) (Credential, error) {
	var out Credential

	for len(src) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(src)
		if tagLen < 0 {
			return Credential{}, fmt.Errorf("credential: bad tag: %w", protowire.ParseError(tagLen))
		}

		src = src[tagLen:]

		var target *string

		switch num {
		case fieldID:
			target = &out.ID
		case fieldCRMID:
			target = &out.CRMID
		case fieldKeyEncrypted:
			target = &out.KeyEncrypted
		default: // a field this version does not know: skipped below
		}

		if target == nil {
			skip := protowire.ConsumeFieldValue(num, typ, src)
			if skip < 0 {
				return Credential{}, fmt.Errorf("credential: bad field %d: %w", num, protowire.ParseError(skip))
			}

			src = src[skip:]

			continue
		}

		if typ != protowire.BytesType {
			return Credential{}, fmt.Errorf("credential: field %d has wire type %d, want a string", num, typ)
		}

		value, valueLen := protowire.ConsumeBytes(src)
		if valueLen < 0 {
			return Credential{}, fmt.Errorf("credential: bad field %d: %w", num, protowire.ParseError(valueLen))
		}

		*target = string(value)
		src = src[valueLen:]
	}

	return out, nil
}
