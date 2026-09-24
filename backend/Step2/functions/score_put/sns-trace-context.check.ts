// SPDX-License-Identifier: MIT-0
// Fails if score_put stops parenting its work to the publisher's `traceparent` from the SNS
// envelope in an SQS record. Run: npx esbuild sns-trace-context.check.ts --bundle --platform=node | node
import { Context, INVALID_SPAN_CONTEXT, SpanKind, SpanOptions, Tracer, propagation, trace } from '@opentelemetry/api';
import { SQSEvent } from 'aws-lambda';
import assert from 'assert';
import { handler } from './app';

// Unreachable endpoint and fake credentials: the DynamoDB call fails fast, locally.
Object.assign(process.env, { AWS_ENDPOINT_URL: 'http://127.0.0.1:9', AWS_REGION: 'eu-central-1', AWS_ACCESS_KEY_ID: 'x', AWS_SECRET_ACCESS_KEY: 'x' });
const traceId = '0af7651916cd43dd8448eb211c80319c';
const noopSpan = trace.wrapSpanContext(INVALID_SPAN_CONTEXT);
propagation.setGlobalPropagator({ inject: () => {}, fields: () => ['traceparent'], extract: (ctx, carrier: any) => {
  const [, tid, spanId, flags] = String(carrier.traceparent).split('-');
  return trace.setSpanContext(ctx, { traceId: tid, spanId, traceFlags: parseInt(flags, 16), isRemote: true });
} });
let recorded: { ctx: Context; options: SpanOptions } | undefined;
trace.setGlobalTracerProvider({ getTracer: () => ({ startSpan: () => noopSpan,
  startActiveSpan: (_name: string, options: SpanOptions, ctx: Context, fn: (span: typeof noopSpan) => unknown) => {
    recorded = { ctx, options };
    return fn(noopSpan);
  } }) as unknown as Tracer });

const envelope = { TopicArn: 'arn:aws:sns:eu-central-1:259033777210:trivia-leaderboard', MessageId: 'm1', Message: '{"gameId":"g1"}',
  MessageAttributes: { traceparent: { Type: 'String', Value: `00-${traceId}-b7ad6b7169203331-01` } } };
handler({ Records: [{ body: JSON.stringify(envelope) }] } as SQSEvent).catch(() => {}).then(() => {
  assert.ok(recorded, 'startActiveSpan was never called');
  assert.strictEqual(trace.getSpanContext(recorded.ctx)?.traceId, traceId);
  assert.strictEqual(recorded.options.kind, SpanKind.CONSUMER);
  assert.ok(!recorded.options.root, 'span is forced to be a trace root');
  console.log('ok: score_put work is parented to the publisher trace');
});
