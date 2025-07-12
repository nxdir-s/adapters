package adapters

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

type ErrNilConfig struct{}

func (e *ErrNilConfig) Error() string {
	return "error nil config"
}

type ErrNilToken struct{}

func (e *ErrNilToken) Error() string {
	return "error nil token"
}

type ErrNilClient struct{}

func (e *ErrNilClient) Error() string {
	return "error nil client"
}

type ErrNilDrive struct{}

func (e *ErrNilDrive) Error() string {
	return "error nil drive"
}

type ErrAuthCode struct {
	err error
}

func (e *ErrAuthCode) Error() string {
	return "unable to read authorization code: " + e.err.Error()
}

type ErrWebToken struct {
	err error
}

func (e *ErrWebToken) Error() string {
	return "unable to retrieve token from web: " + e.err.Error()
}

type ErrDriveClient struct {
	err error
}

func (e *ErrDriveClient) Error() string {
	return "unable to retrieve drive client: " + e.err.Error()
}

type ErrOpenFile struct {
	err    error
	source string
}

func (e *ErrOpenFile) Error() string {
	return "error opening " + e.source + " : " + e.err.Error()
}

type ErrDriveUpload struct {
	err error
}

func (e *ErrDriveUpload) Error() string {
	return "error uploading to drive: " + e.err.Error()
}

type GoogleOpt func(a *GoogleAdapter) error

func WithCredentialsJSON(ctx context.Context, source string) GoogleOpt {
	return func(a *GoogleAdapter) error {
		credentialsJSON, err := os.ReadFile(source)
		if err != nil {
			return err
		}

		config, err := google.ConfigFromJSON(credentialsJSON)
		if err != nil {
			return err
		}

		a.config = config

		fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", a.config.AuthCodeURL("state-token", oauth2.AccessTypeOffline))

		var authCode string
		if _, err := fmt.Scan(&authCode); err != nil {
			return &ErrAuthCode{err}
		}

		token, err := a.config.Exchange(ctx, authCode)
		if err != nil {
			return &ErrWebToken{err}
		}

		a.token = token
		a.client = a.config.Client(ctx, a.token)

		return nil
	}
}

func WithDriveConn(ctx context.Context) GoogleOpt {
	return func(a *GoogleAdapter) error {
		if a.client == nil {
			return &ErrNilClient{}
		}

		client, err := drive.NewService(ctx, option.WithHTTPClient(a.client))
		if err != nil {
			return &ErrDriveClient{err}
		}

		a.drive = client

		return nil
	}
}

func WithGoogleSpanAttrs(attrs ...attribute.KeyValue) GoogleOpt {
	return func(a *GoogleAdapter) error {
		a.attributes = append(a.attributes, attrs...)
		return nil
	}
}

type GoogleAdapter struct {
	config     *oauth2.Config
	token      *oauth2.Token
	client     *http.Client
	drive      *drive.Service
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

func NewGoogleAdapter(tracer trace.Tracer, opts ...GoogleOpt) (*GoogleAdapter, error) {
	adapter := &GoogleAdapter{
		tracer: tracer,
	}

	for _, opt := range opts {
		if err := opt(adapter); err != nil {
			return nil, err
		}
	}

	return adapter, nil
}

func (a *GoogleAdapter) Upload(ctx context.Context, file io.Reader, name string) error {
	if a.drive == nil {
		return &ErrNilDrive{}
	}

	ctx, span := a.tracer.Start(ctx, "Google Upload",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("file.name", name),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	df := &drive.File{
		Name: name,
	}

	if _, err := a.drive.Files.Create(df).Media(file).Do(); err != nil {
		return &ErrDriveUpload{err}
	}

	return nil
}
