package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "embed"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

//go:embed testdata/object.json
var testObject []byte

type mockS3Opt func(m *mockS3Client)

func withHeadObject(returnArgs ...interface{}) mockS3Opt {
	return func(m *mockS3Client) {
		m.On("HeadObject", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withGetObject(returnArgs ...interface{}) mockS3Opt {
	return func(m *mockS3Client) {
		m.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withPutObject(returnArgs ...interface{}) mockS3Opt {
	return func(m *mockS3Client) {
		m.On("PutObject", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withListObjectsV2(returnArgs ...interface{}) mockS3Opt {
	return func(m *mockS3Client) {
		m.On("ListObjectsV2", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withDeleteObjects(returnArgs ...interface{}) mockS3Opt {
	return func(m *mockS3Client) {
		m.On("DeleteObjects", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

type mockS3Client struct {
	mock.Mock
}

func newMockS3Client(opts ...mockS3Opt) *mockS3Client {
	s3 := &mockS3Client{}

	for _, opt := range opts {
		opt(s3)
	}

	return s3
}

func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.HeadObjectOutput), args.Error(1)
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.GetObjectOutput), args.Error(1)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.PutObjectOutput), args.Error(1)
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.ListObjectsV2Output), args.Error(1)
}

func (m *mockS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*s3.DeleteObjectsOutput), args.Error(1)
}

type mockObjectNotExistsWaiter struct {
	expectedErr error
}

func (m *mockObjectNotExistsWaiter) Wait(ctx context.Context, params *s3.HeadObjectInput, maxWaitDur time.Duration, optFns ...func(*s3.ObjectNotExistsWaiterOptions)) error {
	return m.expectedErr
}

type ErrNoPages struct{}

func (e *ErrNoPages) Error() string {
	return "no more pages"
}

type mockListObjectsV2Pager struct {
	PageNum int
	Pages   []*s3.ListObjectsV2Output
}

func (m *mockListObjectsV2Pager) HasMorePages() bool {
	return m.PageNum < len(m.Pages)
}

func (m *mockListObjectsV2Pager) NextPage(ctx context.Context, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.PageNum >= len(m.Pages) {
		return nil, &ErrNoPages{}
	}

	output := m.Pages[m.PageNum]
	m.PageNum++

	return output, nil
}

type ErrTest struct{}

func (e *ErrTest) Error() string {
	return "test error"
}

type TestObject struct {
	Key string `json:"key"`
}

const (
	TestArn     string = "arn:partition:service:region:account-id:resource-id"
	TestKey     string = "object.json"
	TestBucket  string = "s3-bucket"
	TestPrefix  string = "obj/tests/"
	TestSecret  string = "secret"
	TestAttrKey string = "test.t"
	TestAttrVal string = "unit"
)

func TestGetObject(t *testing.T) {
	cases := []struct {
		key         string
		bucket      string
		expectedErr error
		opts        []AWSOpt
	}{
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withGetObject(&s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(testObject)),
					}, nil),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: &ErrGetObject{&ErrTest{}},
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withGetObject(&s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader([]byte{})),
					}, &ErrTest{}),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: &ErrNilAWSClient{"s3"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var object TestObject
	if err := json.NewDecoder(bytes.NewReader(testObject)).Decode(&object); err != nil {
		t.Fatal(err.Error())
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			out, err := adapter.GetObject(ctx, tt.bucket, tt.key)

			assert.Equal(t, tt.expectedErr, err)
			if err == nil {
				var outObject TestObject
				if err := json.NewDecoder(out).Decode(&outObject); err != nil {
					t.Fatal(err.Error())
				}

				assert.True(t, reflect.DeepEqual(object, outObject))
			}
		})
	}
}

func TestPutObject(t *testing.T) {
	cases := []struct {
		key         string
		bucket      string
		expectedErr error
		opts        []AWSOpt
	}{
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withPutObject(&s3.PutObjectOutput{}, nil),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: &ErrPutObject{&ErrTest{}},
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withPutObject(&s3.PutObjectOutput{}, &ErrTest{}),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: &ErrNilAWSClient{"s3"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			err := adapter.PutObject(ctx, bytes.NewReader(testObject), tt.bucket, tt.key)

			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestObjectExists(t *testing.T) {
	cases := []struct {
		key         string
		bucket      string
		expectedVal int
		expectedErr error
		opts        []AWSOpt
	}{
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedVal: S3ObjectExists,
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withHeadObject(&s3.HeadObjectOutput{}, nil),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedVal: S3ObjectNotExists,
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withHeadObject(&s3.HeadObjectOutput{}, &smithy.GenericAPIError{
						Code: "NotFound",
					}),
				)),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedVal: S3ObjectNotExists,
			expectedErr: &ErrHeadObject{&ErrTest{}},
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withHeadObject(&s3.HeadObjectOutput{}, &ErrTest{}),
				)),
			},
		},
		{
			key:         TestKey,
			bucket:      TestBucket,
			expectedErr: &ErrNilAWSClient{"s3"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			out, err := adapter.ObjectExists(ctx, tt.bucket, tt.key)

			assert.Equal(t, tt.expectedErr, err)
			if err == nil {
				assert.Equal(t, tt.expectedVal, out)
			}
		})
	}
}

func TestListObjects(t *testing.T) {
	var keyCount int32 = 3
	var testKey string = TestKey

	cases := []struct {
		bucket        string
		prefix        string
		expectedErr   error
		opts          []AWSOpt
		expectedCount int
		pager         ListObjectsV2Pager
	}{
		{
			bucket:      TestBucket,
			prefix:      TestPrefix,
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client()),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
			expectedCount: 9,
			pager: &mockListObjectsV2Pager{
				Pages: []*s3.ListObjectsV2Output{
					{
						Contents: genData(int(keyCount), types.Object{
							Key: &testKey,
						}),
						KeyCount: &keyCount,
					},
					{
						Contents: genData(int(keyCount), types.Object{
							Key: &testKey,
						}),
						KeyCount: &keyCount,
					},
					{
						Contents: genData(int(keyCount), types.Object{
							Key: &testKey,
						}),
						KeyCount: &keyCount,
					},
				},
			},
		},
		{
			bucket:      TestBucket,
			prefix:      TestPrefix,
			expectedErr: &ErrNilAWSClient{"s3"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			out, err := adapter.ListObjects(ctx, tt.bucket, tt.prefix, tt.pager)

			assert.Equal(t, tt.expectedErr, err)
			if err == nil {
				assert.Equal(t, tt.expectedCount, len(out))
			}
		})
	}
}

func TestDeleteObjects(t *testing.T) {
	var keyCount int32 = 3
	var testKey string = TestKey

	cases := []struct {
		bucket      string
		objectKeys  []string
		waiter      ObjectNotExistsWaiter
		expectedErr error
		opts        []AWSOpt
	}{
		{
			bucket:     TestBucket,
			objectKeys: genData(int(keyCount), testKey),
			waiter: &mockObjectNotExistsWaiter{
				expectedErr: nil,
			},
			expectedErr: nil,
			opts: []AWSOpt{
				WithS3Client(newMockS3Client(
					withDeleteObjects(&s3.DeleteObjectsOutput{
						Deleted: genData(int(keyCount), types.DeletedObject{
							Key: &testKey,
						}),
					}, nil),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			bucket:      TestBucket,
			objectKeys:  genData(0, testKey),
			waiter:      nil,
			expectedErr: &ErrNilAWSClient{"s3"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			err := adapter.DeleteObjects(ctx, tt.bucket, tt.objectKeys, tt.waiter)

			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func genData[T any](count int, obj T) []T {
	data := make([]T, 0, count)

	for _ = range count {
		data = append(data, obj)
	}

	return data
}

type mockSecretsOpt func(m *mockSecretsClient)

func withGetSecretValue(returnArgs ...interface{}) mockSecretsOpt {
	return func(m *mockSecretsClient) {
		m.On("GetSecretValue", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

type mockSecretsClient struct {
	mock.Mock
}

func (m *mockSecretsClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*secretsmanager.GetSecretValueOutput), args.Error(1)
}

func newMockSecretsClient(opts ...mockSecretsOpt) *mockSecretsClient {
	client := &mockSecretsClient{}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

func TestGetSecretValue(t *testing.T) {
	var testSecret string = TestSecret

	cases := []struct {
		arn         string
		name        string
		expectedErr error
		opts        []AWSOpt
	}{
		{
			arn:         TestArn,
			name:        TestKey,
			expectedErr: nil,
			opts: []AWSOpt{
				WithSecretsManagerClient(newMockSecretsClient(
					withGetSecretValue(&secretsmanager.GetSecretValueOutput{
						SecretString: &testSecret,
					}, nil),
				)),
				WithAWSSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			arn:         TestArn,
			name:        TestKey,
			expectedErr: &ErrGetSecret{&ErrTest{}},
			opts: []AWSOpt{
				WithSecretsManagerClient(newMockSecretsClient(
					withGetSecretValue(&secretsmanager.GetSecretValueOutput{}, &ErrTest{}),
				)),
			},
		},
		{
			arn:         TestArn,
			name:        TestKey,
			expectedErr: &ErrNilAWSClient{"secretsmanager"},
			opts:        []AWSOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewAWSAdapter(logger, otel.Tracer("aws"), tt.opts...)

			out, err := adapter.GetSecret(ctx, tt.arn, tt.name)

			assert.Equal(t, tt.expectedErr, err)
			if err == nil {
				assert.Equal(t, testSecret, out)
			}
		})
	}
}

type ErrMissingMsg struct {
	errType string
}

func (e *ErrMissingMsg) Error() string {
	return "missing error message for " + e.errType
}

func genErr[T any]() *T {
	return new(T)
}

func TestAWSErrors(t *testing.T) {
	var err error

	err = &ErrNilAWSClient{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNilAWSClient"})
	}

	err = &ErrTypeCast{"test"}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrTypeCast"})
	}

	err = &ErrGetObject{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrGetObject"})
	}

	err = &ErrPutObject{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrPutObject"})
	}

	err = &ErrHeadObject{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrHeadObject"})
	}

	err = &ErrListObjects{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrListObjects"})
	}

	err = &ErrDeleteObjects{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrDeleteObjects"})
	}

	err = &ErrS3Output{"test"}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrS3Output"})
	}

	err = &ErrGetSecret{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrGetSecret"})
	}
}
