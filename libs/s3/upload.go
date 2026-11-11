package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// maxUploadParts is S3's limit on the parts of one multipart upload.
const maxUploadParts = 10_000

// Upload writes one object of unknown size as a stream: an [io.Writer] that
// buffers Config.UploadPartBytes and sends each full buffer as a multipart
// part, so memory stays at one part however large the object grows. An object
// that never fills a part is stored with a single put instead.
//
// The object appears at its key only when [Upload.Close] returns nil — a
// reader never sees half of it. After a failed Write or Close, or instead of
// Close, call [Upload.Abort] to discard the parts already sent; a process that
// dies first leaves them for the bucket's "abort incomplete multipart
// uploads" lifecycle rule.
//
// An Upload is not safe for concurrent use.
type Upload struct {
	client      *Client
	ctx         context.Context //nolint:containedctx // an io.Writer has no ctx parameter; the upload lives as long as the caller's operation
	key         string
	contentType string

	buf      bytes.Buffer
	uploadID string
	parts    []types.CompletedPart
	size     int64
	done     bool
}

// NewUpload starts a streaming upload to key. Nothing is sent until the first
// part fills or Close is called. ctx bounds the whole upload; every part
// additionally has Config.TransferTimeout.
func (c *Client) NewUpload(ctx context.Context, key, contentType string) *Upload {
	return &Upload{client: c, ctx: ctx, key: key, contentType: contentType}
}

// Size is the number of bytes written so far.
func (u *Upload) Size() int64 { return u.size }

// Write buffers p, sending a part each time the buffer fills.
func (u *Upload) Write(p []byte) (int, error) {
	if u.done {
		return 0, errors.New("s3: write to a finished upload")
	}

	partSize := int(u.client.cfg.UploadPartBytes)
	written := 0

	for len(p) > 0 {
		chunk := min(partSize-u.buf.Len(), len(p))
		u.buf.Write(p[:chunk])

		p = p[chunk:]
		written += chunk
		u.size += int64(chunk)

		if u.buf.Len() == partSize {
			if err := u.flushPart(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

// flushPart sends the buffer as the next part, starting the multipart upload
// on the first one.
func (u *Upload) flushPart() error {
	cfg := u.client.cfg

	if u.uploadID == "" {
		err := u.client.call(u.ctx, opCreateUpload, u.key, cfg.OpTimeout, false, func(ctx context.Context) error {
			out, err := u.client.api.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
				Bucket: aws.String(cfg.Bucket), Key: aws.String(u.key), ContentType: contentTypeOrNil(u.contentType),
			})
			if err != nil {
				return err
			}

			u.uploadID = aws.ToString(out.UploadId)

			return nil
		})
		if err != nil {
			return fmt.Errorf("s3: start upload of %q: %w", u.key, err)
		}
	}

	number := int32(len(u.parts) + 1) //nolint:gosec // bounded by maxUploadParts just below
	if number > maxUploadParts {
		return fmt.Errorf("s3: upload of %q needs more than %d parts: raise S3_UPLOAD_PART_BYTES", u.key, maxUploadParts)
	}

	body := u.buf.Bytes()

	err := u.client.call(u.ctx, opUploadPart, u.key, cfg.TransferTimeout, false, func(ctx context.Context) error {
		out, err := u.client.api.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket: aws.String(cfg.Bucket), Key: aws.String(u.key), UploadId: aws.String(u.uploadID),
			PartNumber: aws.Int32(number), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		})
		if err != nil {
			return err
		}

		u.parts = append(u.parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})

		return nil
	})
	if err != nil {
		return fmt.Errorf("s3: upload part %d of %q: %w", number, u.key, err)
	}

	u.buf.Reset()

	return nil
}

// Close sends what is buffered and makes the object visible. On error the
// object does not exist; call [Upload.Abort] to discard the parts.
func (u *Upload) Close() error {
	if u.done {
		return errors.New("s3: upload already finished")
	}

	if u.uploadID == "" {
		// Never filled a part: one put, no multipart bookkeeping.
		if err := u.client.Put(u.ctx, u.key, u.buf.Bytes(), u.contentType); err != nil {
			return err
		}

		u.finish()

		return nil
	}

	if u.buf.Len() > 0 {
		if err := u.flushPart(); err != nil {
			return err
		}
	}

	cfg := u.client.cfg

	err := u.client.call(u.ctx, opCompleteUpload, u.key, cfg.TransferTimeout, false, func(ctx context.Context) error {
		_, err := u.client.api.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
			Bucket: aws.String(cfg.Bucket), Key: aws.String(u.key), UploadId: aws.String(u.uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts},
		})

		return err
	})
	if err != nil {
		return fmt.Errorf("s3: complete upload of %q: %w", u.key, err)
	}

	u.finish()

	return nil
}

func (u *Upload) finish() {
	u.done = true
	u.buf = bytes.Buffer{} // release the part buffer
}

// Abort discards an upload that will not be completed. It is a no-op after a
// successful Close and safe to defer. It runs detached from the upload's
// context: the usual reason to abort is that this context has just ended.
func (u *Upload) Abort() {
	if u.done {
		return
	}

	u.finish()

	if u.uploadID == "" {
		return
	}

	cfg := u.client.cfg
	ctx := context.WithoutCancel(u.ctx)

	err := u.client.call(ctx, opAbortUpload, u.key, cfg.OpTimeout, false, func(ctx context.Context) error {
		_, err := u.client.api.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
			Bucket: aws.String(cfg.Bucket), Key: aws.String(u.key), UploadId: aws.String(u.uploadID),
		})

		return err
	})
	if err != nil {
		u.client.log.WarnContext(ctx, "s3 multipart upload not aborted: its parts stay until the bucket's lifecycle rule removes them",
			"key", u.key, "upload_id", u.uploadID, "err", err)
	}
}
