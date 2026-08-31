import assert from "node:assert/strict";
import test from "node:test";

import {
  FrameDecoder,
  decodeJSON,
  encodeFrame,
  encodeEvidenceHeader,
  encodeFramePayload,
  MaxControlFrameLength,
} from "./protocol.ts";

test("frames UTF-8 JSON with an unsigned big-endian length", () => {
  const value = { message: "hello\n世界" };
  const frame = encodeFrame(value);
  const payload = new TextEncoder().encode(JSON.stringify(value));

  assert.equal(new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getBigUint64(0), BigInt(payload.byteLength));
  assert.deepEqual(frame.slice(8), payload);
});

test("preserves provided control bytes exactly", () => {
  const payload = new Uint8Array([0, 10, 240, 159, 140, 141]);
  const frame = encodeFramePayload(payload);

  assert.deepEqual(frame.slice(8), payload);
});

test("encodes the shared protocol golden vector for the evidence header", () => {
  // Shared Go/TypeScript protocol vector: uint64(0x0102030405060708) is big-endian.
  const declaredLength = 0x0102030405060708n;
  const expectedHeader = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

  assert.deepEqual(encodeEvidenceHeader(declaredLength), expectedHeader);
});

test("creates an evidence header without allocating a payload frame", () => {
  const header = encodeEvidenceHeader(2n ** 63n);

  assert.equal(header.byteLength, 8);
  assert.equal(new DataView(header.buffer, header.byteOffset, header.byteLength).getBigUint64(0), 2n ** 63n);
});

test("decodes fragmented and coalesced frames while retaining a trailing frame", () => {
  const first = encodeFrame({ request_id: "one", body: { line: "a\nb" } });
  const second = encodeFrame({ request_id: "two", body: { text: "世界" } });
  const decoder = new FrameDecoder();

  assert.deepEqual(decoder.push(first.slice(0, 5)), []);
  const decoded = decoder.push(concat(first.slice(5), second, first.slice(0, 3)));
  assert.equal(decoded.length, 2);
  assert.deepEqual(decodeJSON(decoded[0]), { request_id: "one", body: { line: "a\nb" } });
  assert.deepEqual(decodeJSON(decoded[1]), { request_id: "two", body: { text: "世界" } });
  assert.equal(decoder.bufferedByteLength, 3);

  assert.deepEqual(decoder.push(first.slice(3)), [first.slice(8)]);
});

test("enforces the bounded control-plane frame limit", () => {
  assert.doesNotThrow(() => encodeFramePayload(new Uint8Array(MaxControlFrameLength)));
  assert.throws(() => encodeFramePayload(new Uint8Array(MaxControlFrameLength + 1)), /control frame length/);

  const tooLarge = new Uint8Array(8);
  new DataView(tooLarge.buffer).setBigUint64(0, BigInt(MaxControlFrameLength) + 1n);
  assert.throws(() => new FrameDecoder().push(tooLarge), /control frame length/);
});

test("rejects malformed or empty JSON payloads", () => {
  assert.throws(() => decodeJSON(new Uint8Array()), SyntaxError);
  assert.throws(() => decodeJSON(new TextEncoder().encode('{"broken":')), SyntaxError);
});

function concat(...chunks) {
  const length = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}
