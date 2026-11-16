package s3

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wundernightmare/golang-basics/libs/secret"
)

func offlineClient(t *testing.T, mutate func(*Config)) *Client {
	t.Helper()

	cfg := Config{Bucket: "events", Endpoint: "https://s3.internal:9000", AccessKeyID: "k", SecretAccessKey: secret.New("s")}
	if mutate != nil {
		mutate(&cfg)
	}

	client, err := New(context.Background(), cfg, nil)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

// A Config built by hand addresses the bucket in the path, which every
// S3-compatible store serves; the bucket moves into the host name only when
// asked. Read off a presigned URL: what the SDK would send, without sending.
func TestBucketAddressing(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mutate     func(*Config)
		host, path string
	}{
		"zero value":         {nil, "s3.internal:9000", "/events/k"},
		"virtual-host style": {func(c *Config) { c.VirtualHostStyle = true }, "events.s3.internal:9000", "/k"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sdk, ok := offlineClient(t, tc.mutate).api.(*awss3.Client)
			require.True(t, ok)

			req, err := awss3.NewPresignClient(sdk).PresignGetObject(context.Background(),
				&awss3.GetObjectInput{Bucket: aws.String("events"), Key: aws.String("k")})
			require.NoError(t, err)

			got, err := url.Parse(req.URL)
			require.NoError(t, err)
			assert.Equal(t, tc.host, got.Host)
			assert.Equal(t, tc.path, got.Path)
		})
	}
}

// bucketless is a store without the bucket that records how it was asked to
// create it. Any other call panics on the nil interface.
type bucketless struct {
	api

	created atomic.Pointer[awss3.CreateBucketInput]
}

func (*bucketless) HeadBucket(context.Context, *awss3.HeadBucketInput, ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NotFound"}
}

func (b *bucketless) CreateBucket(_ context.Context, in *awss3.CreateBucketInput, _ ...func(*awss3.Options)) (*awss3.CreateBucketOutput, error) {
	b.created.Store(in)

	return &awss3.CreateBucketOutput{}, nil
}

// AWS creates a bucket outside us-east-1 only when the request names the
// region, and refuses one that names us-east-1. An S3-compatible store has
// one region and is not told.
func TestEnsureNamesTheRegionOfANewAWSBucket(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		endpoint, region string
		want             types.BucketLocationConstraint
	}{
		"aws, another region":    {"", "eu-west-1", types.BucketLocationConstraintEuWest1},
		"aws, us-east-1":         {"", "us-east-1", ""},
		"compatible, any region": {"https://s3.internal:9000", "eu-west-1", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := &bucketless{}
			client := offlineClient(t, func(c *Config) {
				c.Endpoint, c.Region, c.AutoCreateBucket = tc.endpoint, tc.region, true
			})
			client.api = store

			require.NoError(t, client.Ensure(context.Background()))

			in := store.created.Load()
			require.NotNil(t, in)

			if tc.want == "" {
				assert.Nil(t, in.CreateBucketConfiguration)
				return
			}

			require.NotNil(t, in.CreateBucketConfiguration)
			assert.Equal(t, tc.want, in.CreateBucketConfiguration.LocationConstraint)
		})
	}
}
