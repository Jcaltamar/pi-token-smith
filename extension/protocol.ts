export const ProtocolVersion = 1 as const;
export type ProtocolVersion = typeof ProtocolVersion;

export type JSONPrimitive = boolean | number | string | null;
export type JSONValue = JSONPrimitive | JSONObject | JSONArray;
export type JSONObject = { readonly [key: string]: JSONValue };
export type JSONArray = readonly JSONValue[];

export interface RequestEnvelope<TBody extends JSONValue = JSONValue> {
  readonly protocol_version: ProtocolVersion;
  readonly request_id: string;
  readonly operation: string;
  readonly sent_at: string;
  readonly body: TBody;
}

export interface ResponseError {
  readonly code: string;
  readonly message: string;
}

export interface ResponseEnvelope<TBody extends JSONValue = JSONValue> {
  readonly protocol_version: ProtocolVersion;
  readonly request_id: string;
  readonly status: string;
  readonly body: TBody;
  readonly error?: ResponseError;
}

const frameHeaderLength = 8;
const maximumEvidenceLength = (1n << 64n) - 1n;

// MaxControlFrameLength bounds metadata-only JSON envelopes. Evidence uses the
// streaming data-plane header below and must not be embedded in these frames.
export const MaxControlFrameLength = 1 << 20;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// encodeFrame uses JSON.stringify without a replacer or whitespace, then UTF-8
// encodes that JSON. Use encodeFramePayload when bytes are already serialized.
export function encodeFrame(value: JSONValue): Uint8Array {
  return encodeFramePayload(textEncoder.encode(JSON.stringify(value)));
}

// encodeFramePayload preserves bounded control-plane payload bytes unchanged.
export function encodeFramePayload(payload: Uint8Array): Uint8Array {
  if (payload.byteLength > MaxControlFrameLength) {
    throw new RangeError("protocol control frame length exceeds safety bound");
  }
  const frame = new Uint8Array(frameHeaderLength + payload.byteLength);
  frame.set(encodeEvidenceHeader(BigInt(payload.byteLength)));
  frame.set(payload, frameHeaderLength);
  return frame;
}

// encodeEvidenceHeader creates only the data-plane header so callers can stream
// evidence bytes directly to a socket without constructing a second full frame.
export function encodeEvidenceHeader(length: bigint): Uint8Array {
  if (length < 0n || length > maximumEvidenceLength) {
    throw new RangeError("protocol evidence length is outside the unsigned 64-bit range");
  }
  const header = new Uint8Array(frameHeaderLength);
  new DataView(header.buffer).setBigUint64(0, length);
  return header;
}

// decodeJSON decodes UTF-8 JSON from one complete frame payload.
export function decodeJSON(payload: Uint8Array): JSONValue {
  return JSON.parse(textDecoder.decode(payload)) as JSONValue;
}

// FrameDecoder accepts arbitrary control-plane socket chunks and returns complete
// bounded payloads. It queues chunks rather than repeatedly concatenating them.
export class FrameDecoder {
  #chunks: Uint8Array[] = [];
  #chunkOffset = 0;
  #bufferedByteLength = 0;

  get bufferedByteLength(): number {
    return this.#bufferedByteLength;
  }

  push(chunk: Uint8Array): Uint8Array[] {
    if (chunk.byteLength > 0) {
      this.#chunks.push(chunk);
      this.#bufferedByteLength += chunk.byteLength;
    }
    const frames: Uint8Array[] = [];

    while (this.#bufferedByteLength >= frameHeaderLength) {
      const header = this.#peek(frameHeaderLength);
      const length = new DataView(header.buffer, header.byteOffset, frameHeaderLength).getBigUint64(0);
      if (length > BigInt(MaxControlFrameLength)) {
        throw new RangeError("protocol control frame length exceeds safety bound");
      }
      const payloadLength = Number(length);
      if (this.#bufferedByteLength < frameHeaderLength + payloadLength) {
        break;
      }
      this.#read(frameHeaderLength);
      frames.push(this.#read(payloadLength));
    }

    if (this.#bufferedByteLength > frameHeaderLength + MaxControlFrameLength) {
      throw new RangeError("protocol control frame buffer exceeds safety bound");
    }
    return frames;
  }

  #peek(length: number): Uint8Array {
    const result = new Uint8Array(length);
    let resultOffset = 0;
    for (let index = 0; index < this.#chunks.length; index += 1) {
      const chunk = this.#chunks[index];
      const start = index === 0 ? this.#chunkOffset : 0;
      const count = Math.min(length - resultOffset, chunk.byteLength - start);
      result.set(chunk.subarray(start, start + count), resultOffset);
      resultOffset += count;
      if (resultOffset === length) {
        return result;
      }
    }
    throw new Error("protocol decoder buffer invariant violated");
  }

  #read(length: number): Uint8Array {
    const result = new Uint8Array(length);
    let offset = 0;
    while (offset < length) {
      const chunk = this.#chunks[0];
      const available = chunk.byteLength - this.#chunkOffset;
      const count = Math.min(length - offset, available);
      result.set(chunk.subarray(this.#chunkOffset, this.#chunkOffset + count), offset);
      offset += count;
      this.#chunkOffset += count;
      this.#bufferedByteLength -= count;
      if (this.#chunkOffset === chunk.byteLength) {
        this.#chunks.shift();
        this.#chunkOffset = 0;
      }
    }
    return result;
  }
}
