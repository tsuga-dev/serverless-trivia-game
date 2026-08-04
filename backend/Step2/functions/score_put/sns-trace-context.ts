// SPDX-License-Identifier: MIT-0
// Joins an SNS-triggered invocation to the trace of whoever published the message.
//
// The OpenTelemetry Lambda layer's default context extractor only reads `event.headers`.
// An SNS event has no headers, so the layer's invocation span is always a fresh trace root
// -- even though the publisher's aws-sdk instrumentation did inject `traceparent` into the
// message attributes. The result, before this file existed: `game-answer` published to two
// topics inside an API request's trace, and the two consumers each began an unrelated
// trace, so nothing showed the request driving three functions.
//
// This does not move the layer's invocation span (that span is created before any handler
// code runs and cannot be reparented). It runs the handler's work in a CONSUMER span
// parented to the publisher instead, so the work -- and every DynamoDB span beneath it --
// appears in the publisher's trace.
//
// `@opentelemetry/api` here is a second copy of the package, not the layer's: the API
// resolves the provider through a versioned global, so a separate copy still talks to the
// layer's SDK. That makes the version range load-bearing. It must stay on the major the
// layer registers into (^1.9.0 as of layer opentelemetry-nodejs-0_19_0) -- a mismatch does
// not throw, `getTracer` just returns a no-op and these spans silently disappear.
import { SpanKind, SpanStatusCode, context, propagation, trace } from '@opentelemetry/api';
import { SNSMessage } from 'aws-lambda';

const tracer = trace.getTracer('sts.sns-consumer');

// SNS message attributes are the carrier the publisher wrote `traceparent` into. Keys are
// lower-cased because the W3C propagator looks for lower-case field names, while SNS
// preserves whatever case the publisher used.
const carrierFrom = (message: SNSMessage): Record<string, string> => {
  const carrier: Record<string, string> = {};
  const attributes = message.MessageAttributes;
  if (!attributes) {
    return carrier;
  }
  for (const name of Object.keys(attributes)) {
    const value = attributes[name]?.Value;
    if (typeof value === 'string') {
      carrier[name.toLowerCase()] = value;
    }
  }
  return carrier;
};

const topicNameFrom = (topicArn: string): string => topicArn.split(':').pop() || topicArn;

export const consumeInPublisherTrace = async <T>(
  message: SNSMessage,
  work: () => Promise<T>,
): Promise<T> => {
  const topic = topicNameFrom(message.TopicArn);
  // Named `<destination> process` to match the messaging semantic conventions, so it pairs
  // with the publisher's `<destination> send` span in a waterfall.
  return tracer.startActiveSpan(
    `${topic} process`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        'messaging.system': 'aws_sns',
        'messaging.operation': 'process',
        'messaging.destination.name': topic,
        'messaging.message.id': message.MessageId,
      },
    },
    propagation.extract(context.active(), carrierFrom(message)),
    async (span) => {
      try {
        return await work();
      } catch (e) {
        span.recordException(e as Error);
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw e;
      } finally {
        span.end();
      }
    },
  );
};
