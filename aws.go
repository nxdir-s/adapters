package adapters

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	MaxDeleteKeys int = 1000
)

type ErrNilAWSClient struct {
	client string
}

func (e *ErrNilAWSClient) Error() string {
	return "missing client: " + e.client
}

type ErrTypeCast struct {
	value string
}

func (e *ErrTypeCast) Error() string {
	return "failed to type cast " + e.value
}

type ErrGetObject struct {
	err error
}

func (e *ErrGetObject) Error() string {
	return "failed to get object from s3: " + e.err.Error()
}

type ErrPutObject struct {
	err error
}

func (e *ErrPutObject) Error() string {
	return "failed to upload to s3: " + e.err.Error()
}

type ErrHeadObject struct {
	err error
}

func (e *ErrHeadObject) Error() string {
	return "failed HeadObject request to s3: " + e.err.Error()
}

type ErrListObjects struct {
	err error
}

func (e *ErrListObjects) Error() string {
	return "failed to list s3 objects: " + e.err.Error()
}

type ErrDeleteObjects struct {
	err error
}

func (e *ErrDeleteObjects) Error() string {
	return "failed to delete s3 objects: " + e.err.Error()
}

type ErrS3Output struct {
	api string
}

func (e *ErrS3Output) Error() string {
	return "found one or more errors in " + e.api + " output"
}

type ErrGetSecret struct {
	err error
}

func (e *ErrGetSecret) Error() string {
	return "failed to get secret: " + e.err.Error()
}

type AWSOpt func(a *AWSAdapter)

func WithS3Client(client S3Client) AWSOpt {
	return func(a *AWSAdapter) {
		a.s3 = client
	}
}

func WithSecretsManagerClient(client SecretsManagerClient) AWSOpt {
	return func(a *AWSAdapter) {
		a.sm = client
	}
}

func WithAWSSpanAttrs(attrs ...attribute.KeyValue) AWSOpt {
	return func(a *AWSAdapter) {
		a.attributes = append(a.attributes, attrs...)
	}
}

type S3Client interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type ListObjectsV2Pager interface {
	HasMorePages() bool
	NextPage(ctx context.Context, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type ObjectNotExistsWaiter interface {
	Wait(ctx context.Context, params *s3.HeadObjectInput, maxWaitDur time.Duration, optFns ...func(*s3.ObjectNotExistsWaiterOptions)) error
}

type SecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type AWSAdapter struct {
	s3         S3Client
	sm         SecretsManagerClient
	logger     *slog.Logger
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

func NewAWSAdapter(logger *slog.Logger, tracer trace.Tracer, opts ...AWSOpt) *AWSAdapter {
	adapter := &AWSAdapter{
		logger: logger,
		tracer: tracer,
	}

	for _, opt := range opts {
		opt(adapter)
	}

	return adapter
}

func (a *AWSAdapter) GetObject(ctx context.Context, bucket string, key string) (io.Reader, error) {
	if a.s3 == nil {
		return nil, &ErrNilAWSClient{"s3"}
	}

	ctx, span := a.tracer.Start(ctx, "S3 GetObject",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.s3.bucket", bucket),
			attribute.String("aws.s3.key", key),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	a.logger.Info("reading object from s3",
		slog.String("bucket", bucket),
		slog.String("key", key),
	)

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := a.s3.GetObject(ctx, params)
	if err != nil {
		a.logger.Error("failed to read object from s3",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.String("err", err.Error()),
		)

		return nil, &ErrGetObject{err}
	}

	pr, pw := io.Pipe()

	go func() {
		if _, err := io.Copy(pw, output.Body); err != nil {
			a.logger.Error("failed to copy GetObject output",
				slog.String("bucket", bucket),
				slog.String("key", key),
				slog.String("err", err.Error()),
			)

			return
		}
	}()

	return pr, nil
}

func (a *AWSAdapter) PutObject(ctx context.Context, data io.Reader, bucket string, key string) error {
	if a.s3 == nil {
		return &ErrNilAWSClient{"s3"}
	}

	ctx, span := a.tracer.Start(ctx, "S3 PutObject",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.s3.bucket", bucket),
			attribute.String("aws.s3.key", key),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	a.logger.Info("uploading to s3",
		slog.String("bucket", bucket),
		slog.String("key", key),
	)

	params := &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 data,
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	}

	if _, err := a.s3.PutObject(ctx, params); err != nil {
		a.logger.Error("failed to upload object to s3",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.String("err", err.Error()),
		)

		return &ErrPutObject{err}

	}

	return nil
}

const (
	S3ObjectNotExists int = iota
	S3ObjectExists
)

func (a *AWSAdapter) ObjectExists(ctx context.Context, bucket string, key string) (int, error) {
	if a.s3 == nil {
		return S3ObjectNotExists, &ErrNilAWSClient{"s3"}
	}

	ctx, span := a.tracer.Start(ctx, "S3 ObjectExists",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.s3.bucket", bucket),
			attribute.String("aws.s3.key", key),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	a.logger.Info("checking if object exists in s3",
		slog.String("bucket", bucket),
		slog.String("key", key),
	)

	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	if _, err := a.s3.HeadObject(ctx, params); err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) && apiError.ErrorCode() == "NotFound" {
			a.logger.Info("object does not exist in s3",
				slog.String("bucket", bucket),
				slog.String("key", key),
			)

			return S3ObjectNotExists, nil
		}

		return S3ObjectNotExists, &ErrHeadObject{err}
	}

	a.logger.Info("object exists in s3",
		slog.String("bucket", bucket),
		slog.String("key", key),
	)

	return S3ObjectExists, nil
}

func (a *AWSAdapter) ListObjects(ctx context.Context, bucket string, prefix string, pager interface{}) ([]string, error) {
	if a.s3 == nil {
		return nil, &ErrNilAWSClient{"s3"}
	}

	ctx, span := a.tracer.Start(ctx, "S3 ListObjects",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.s3.bucket", bucket),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	a.logger.Info("listing objects in s3",
		slog.String("bucket", bucket),
		slog.String("prefix", prefix),
	)

	objects := make([]string, 0)
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	if len(prefix) > 0 {
		params.Prefix = &prefix
		span.SetAttributes(attribute.String("aws.s3.prefix", prefix))
	}

	var objectPaginator ListObjectsV2Pager
	objectPaginator = s3.NewListObjectsV2Paginator(a.s3, params)

	if pager != nil {
		v2Pager, ok := pager.(ListObjectsV2Pager)
		if !ok {
			return nil, &ErrTypeCast{"ListObjectsV2Pager"}
		}

		objectPaginator = v2Pager
	}

	for objectPaginator.HasMorePages() {
		output, err := objectPaginator.NextPage(ctx)
		if err != nil {
			a.logger.Error("failed to list s3 objects",
				slog.String("bucket", bucket),
				slog.String("prefix", prefix),
				slog.String("err", err.Error()),
			)

			return nil, &ErrListObjects{err}
		}

		for i := range output.Contents {
			objects = append(objects, *output.Contents[i].Key)
		}
	}

	a.logger.Info("objects found",
		slog.String("bucket", bucket),
		slog.String("prefix", prefix),
		slog.Int("count", len(objects)),
	)

	return objects, nil
}

func (a *AWSAdapter) DeleteObjects(ctx context.Context, bucket string, objects []string, waiter interface{}) error {
	if a.s3 == nil {
		return &ErrNilAWSClient{"s3"}
	}

	if len(objects) == 0 {
		return nil
	}

	ctx, span := a.tracer.Start(ctx, "S3 DeleteObjects",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.s3.bucket", bucket),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	a.logger.Info("deleting objects from s3",
		slog.String("bucket", bucket),
		slog.Int("count", len(objects)),
	)

	objectIds := make([]types.ObjectIdentifier, 0, len(objects))
	for i := range objects {
		objectIds = append(objectIds, types.ObjectIdentifier{
			Key: aws.String(objects[i]),
		})
	}

	deleteIndex := 0
	objectsExist := true

	for objectsExist {
		objectKeys := getDeleteList(objectIds, deleteIndex)

		params := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectKeys,
				Quiet:   aws.Bool(true),
			},
		}

		output, err := a.s3.DeleteObjects(ctx, params)
		if err != nil {
			return &ErrDeleteObjects{err}
		}

		if len(output.Errors) > 0 {
			for i := range output.Errors {
				a.logger.Error("failed to delete s3 object",
					slog.String("bucket", bucket),
					slog.String("key", *output.Errors[i].Key),
					slog.String("err", *output.Errors[i].Message),
				)
			}

			return &ErrDeleteObjects{
				&ErrS3Output{"DeleteObjects"},
			}
		}

		for i := range output.Deleted {
			params := &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    output.Deleted[i].Key,
			}

			var s3Waiter ObjectNotExistsWaiter
			s3Waiter = s3.NewObjectNotExistsWaiter(a.s3)

			if waiter != nil {
				objectWaiter, ok := waiter.(ObjectNotExistsWaiter)
				if !ok {
					return &ErrTypeCast{"ObjectNotExistsWaiter"}
				}

				s3Waiter = objectWaiter
			}

			if err := s3Waiter.Wait(ctx, params, time.Minute); err != nil {
				a.logger.Warn("timed out waiting for object deletion",
					slog.String("bucket", bucket),
					slog.String("key", *output.Deleted[i].Key),
				)

				continue
			}

			a.logger.Info("object deleted",
				slog.String("bucket", bucket),
				slog.String("key", *output.Deleted[i].Key),
			)
		}

		deleteIndex += len(objectKeys)
		if deleteIndex >= len(objectKeys) {
			objectsExist = false
		}
	}

	return nil
}

func getDeleteList(objectIds []types.ObjectIdentifier, index int) []types.ObjectIdentifier {
	if len(objectIds) > MaxDeleteKeys {
		if index+MaxDeleteKeys > len(objectIds) {
			return objectIds[index:]
		}

		return objectIds[index : index+MaxDeleteKeys]
	}

	return objectIds
}

func (a *AWSAdapter) GetSecret(ctx context.Context, arn string, name string) (string, error) {
	if a.sm == nil {
		return "", &ErrNilAWSClient{"secretsmanager"}
	}

	ctx, span := a.tracer.Start(ctx, "SecretsManager GetSecretValue",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("aws.secretsmanager.secretName", name),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	params := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(arn),
	}

	a.logger.Info("retrieving secret from secrets manager",
		slog.String("secretName", name),
	)

	output, err := a.sm.GetSecretValue(ctx, params)
	if err != nil {
		return "", &ErrGetSecret{err}
	}

	return *output.SecretString, nil
}
