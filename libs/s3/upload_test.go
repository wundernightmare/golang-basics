package s3_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/libs/s3"
)

// After a failed Write the upload is spoiled: Close publishes nothing — a
// caller that closes unconditionally must not leave a truncated object — and
// neither Close nor another Write hides the failure.
func TestUploadDoesNotPublishAfterAFailedWrite(t *testing.T) {
	t.Parallel()

	for name, failing := range map[string]string{
		"the upload could not start": "create_upload",
		"a part was refused":         "upload_part",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newFakeStore(t).fails(failing)
			up := store.client(t).NewUpload(context.Background(), "k", "")

			_, writeErr := up.Write(make([]byte, s3.MinUploadPartBytes))
			require.Error(t, writeErr)

			store.answer(failing, nil) // the store is healthy again

			_, err := up.Write([]byte("more"))
			require.ErrorIs(t, err, writeErr)
			require.ErrorIs(t, up.Close(), writeErr)
			assert.Zero(t, store.calls("put")+store.calls("complete_upload"), "nothing was published")

			up.Abort()
		})
	}
}

// Abort tells the store to drop the parts, and does so although the upload's
// context has ended — the usual reason to abort.
func TestAbortDiscardsThePartsAfterTheContextEnded(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	up := store.client(t).NewUpload(ctx, "k", "")
	_, err := up.Write(make([]byte, s3.MinUploadPartBytes))
	require.NoError(t, err)

	cancel()
	up.Abort()
	up.Abort()

	assert.Equal(t, 1, store.calls("abort_upload"))
}

// The content type reaches the store on both ways an object is written.
func TestContentTypeIsSent(t *testing.T) {
	t.Parallel()

	store := newFakeStore(t)
	types := make(chan string, 2)
	record := func(w http.ResponseWriter, r *http.Request) {
		types <- r.Header.Get("Content-Type")

		xml(w, http.StatusOK, initiated)
	}
	store.answer("put", record).answer("create_upload", record)

	client := store.client(t)
	require.NoError(t, client.Put(context.Background(), "k", []byte("x"), "application/zstd"))

	up := client.NewUpload(context.Background(), "k", "text/csv")
	_, err := up.Write(make([]byte, s3.MinUploadPartBytes))
	require.NoError(t, err)
	up.Abort()

	assert.Equal(t, "application/zstd", <-types)
	assert.Equal(t, "text/csv", <-types)
}
