package adapters

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	MaxPollFetches int = 1000
)

type ErrProtoMarshal struct {
	err error
}

func (e *ErrProtoMarshal) Error() string {
	return "failed to marshal protobuf: " + e.err.Error()
}

type ErrCreateTopic struct {
	err error
}

func (e *ErrCreateTopic) Error() string {
	return "failed to create kafka topic: " + e.err.Error()
}

type ErrListTopics struct {
	err error
}

func (e *ErrListTopics) Error() string {
	return "failed to list kafka topics: " + e.err.Error()
}

type KafkaOpt func(a *KafkaAdapter) error

func WithConsumer(topic string, groupname string, brokers []string, opts ...kgo.Opt) KafkaOpt {
	return func(a *KafkaAdapter) error {
		options := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(groupname),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
		}

		options = append(options, opts...)

		client, err := kgo.NewClient(options...)
		if err != nil {
			return err
		}

		a.topic = topic
		a.client = client
		a.groupName = groupname

		return nil
	}
}

func WithProducer(topic string, brokers []string, opts ...kgo.Opt) KafkaOpt {
	return func(a *KafkaAdapter) error {
		options := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
		}

		options = append(options, opts...)

		client, err := kgo.NewClient(options...)
		if err != nil {
			return err
		}

		a.topic = topic
		a.client = client

		return nil
	}
}

func WithKafkaSpanAttrs(attrs ...attribute.KeyValue) KafkaOpt {
	return func(a *KafkaAdapter) error {
		a.attributes = append(a.attributes, attrs...)
		return nil
	}
}

type KafkaConsumer interface {
	Process(ctx context.Context, record *kgo.Record) error
}

type KafkaAdapter struct {
	client     *kgo.Client
	logger     *slog.Logger
	tracer     trace.Tracer
	attributes []attribute.KeyValue
	topic      string
	groupName  string
}

// NewKafkaAdapter creates a new kafka adapter. Adapters should be configured to either produce or consume, but not both
func NewKafkaAdapter(logger *slog.Logger, tracer trace.Tracer, opts ...KafkaOpt) (*KafkaAdapter, error) {
	adapter := &KafkaAdapter{
		logger: logger,
		tracer: tracer,
	}

	for _, opt := range opts {
		if err := opt(adapter); err != nil {
			return nil, err
		}
	}

	return adapter, nil
}

// Send sends the supplied kafka record to the configured topic
func (a *KafkaAdapter) Send(ctx context.Context, record protoreflect.ProtoMessage) error {
	if a.client == nil {
		a.logger.Error("nil client in KafkaAdapter")
		return nil
	}

	ctx, span := a.tracer.Start(ctx, "send "+a.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", a.topic),
			attribute.String("messaging.operation.name", "send"),
			attribute.String("messaging.operation.type", "send"),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	data, err := proto.Marshal(record)
	if err != nil {
		a.logger.Error("error encoding record",
			slog.String("err", err.Error()),
			slog.String("topic", a.topic),
		)

		return &ErrProtoMarshal{err}
	}

	a.logger.Debug("sending kafka record")

	a.client.Produce(ctx, &kgo.Record{Topic: a.topic, Value: data}, func(_ *kgo.Record, err error) {
		if err != nil {
			a.logger.Error("record had a produce error", slog.String("err", err.Error()))
		}
	})

	return nil
}

// Consume uses the supplied consumer to process kafka records from the configured topic
func (a *KafkaAdapter) Consume(ctx context.Context, consumer KafkaConsumer) {
	if a.client == nil {
		a.logger.Error("nil client in KafkaAdapter")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ctx, span := a.tracer.Start(ctx, "receive "+a.topic,
				trace.WithSpanKind(trace.SpanKindClient),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.consumer.group.name", a.groupName),
					attribute.String("messaging.operation.name", "receive"),
					attribute.String("messaging.operation.type", "receive"),
				),
			)

			if a.attributes != nil {
				span.SetAttributes(a.attributes...)
			}

			fetches := a.client.PollRecords(ctx, MaxPollFetches)
			span.End()

			ctx, span = a.tracer.Start(ctx, "process "+a.topic,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.consumer.group.name", a.groupName),
					attribute.String("messaging.operation.name", "process"),
					attribute.String("messaging.operation.type", "process"),
				),
			)

			if a.attributes != nil {
				span.SetAttributes(a.attributes...)
			}

			if errors := fetches.Errors(); len(errors) > 0 {
				for _, e := range errors {
					if e.Err == context.Canceled {
						a.logger.Error("received interrupt", slog.String("err", e.Err.Error()))
						span.End()

						return
					}

					a.logger.Error("poll error", slog.String("err", e.Err.Error()))
				}
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()

				if err := consumer.Process(ctx, record); err != nil {
					a.logger.Error("error processing "+a.topic+" record", slog.String("err", err.Error()))
					span.End()

					return // return or continue?
				}

				a.logger.Info("consumed record", slog.String("topic", a.topic))
			}

			if err := a.client.CommitUncommittedOffsets(ctx); err != nil {
				if err == context.Canceled {
					a.logger.Error("received interrupt", slog.String("err", err.Error()))
					span.End()

					return
				}

				a.logger.Error("unable to commit offsets", slog.String("err", err.Error()))
			}

			a.client.AllowRebalance()
			span.End()
		}
	}
}

// Close closes the kafka client
func (a *KafkaAdapter) Close() error {
	if a.client == nil {
		return nil
	}

	a.client.Close()

	return nil
}

// CreateTopic creates a kafka topic
func (a *KafkaAdapter) CreateTopic(ctx context.Context, topic string) error {
	if a.client == nil {
		a.logger.Error("nil client in KafkaAdapter")
		return nil
	}

	ctx, span := a.tracer.Start(ctx, "create topic "+a.topic,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.name", "create topic"),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	adminClient := kadm.NewClient(a.client)

	topicDetails, err := adminClient.ListTopics(ctx)
	if err != nil {
		return &ErrListTopics{err}
	}

	if topicDetails.Has(topic) {
		a.logger.Warn("kafka topic already exists", slog.String("topic", topic))
		return nil
	}

	a.logger.Info("creating kafka topic", slog.String("topic", topic))

	if _, err := adminClient.CreateTopic(ctx, 1, -1, nil, topic); err != nil {
		a.logger.Error("failed to create kafka topic",
			slog.String("err", err.Error()),
			slog.String("topic", topic),
		)

		return &ErrCreateTopic{err}
	}

	return nil
}
