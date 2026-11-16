package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// S3 takes at most 10 000 parts: the part after the last one is refused
// before anything is sent, with the key that raises the ceiling.
func TestUploadRefusesAPartPastS3sLimit(t *testing.T) {
	t.Parallel()

	up := &Upload{
		client:   &Client{cfg: Config{UploadPartBytes: MinUploadPartBytes}}, // no SDK client: a part that is sent panics
		uploadID: "u",
		parts:    make([]types.CompletedPart, maxUploadParts),
	}

	_, err := up.Write(make([]byte, MinUploadPartBytes))
	require.ErrorContains(t, err, "S3_UPLOAD_PART_BYTES")
}
