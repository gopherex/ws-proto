import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import { FrameSchema, StatusSchema, Kind as GenKind } from "./gen/transport_pb.js";
import type { Frame, Status } from "./gen/transport_pb.js";

export type { Frame, Status };

/**
 * Kind mirrors the wire enum `wsproto.transport.v1.Kind`. protoc-gen-es strips
 * the `KIND_` prefix from generated enum members (emitting `Kind.OPEN` etc.),
 * but the public/stable member names used throughout this runtime and by the
 * Plan 4 generator are the full `KIND_*` names. We therefore re-export a const
 * object whose members carry the FULL wire names and the IDENTICAL numeric
 * values, so the wire contract is unchanged.
 */
export const Kind = {
  KIND_UNSPECIFIED: GenKind.UNSPECIFIED,
  KIND_OPEN: GenKind.OPEN,
  KIND_MSG: GenKind.MSG,
  KIND_HALF_CLOSE: GenKind.HALF_CLOSE,
  KIND_END: GenKind.END,
  KIND_RST: GenKind.RST,
  KIND_HEADER: GenKind.HEADER,
} as const;

export type Kind = (typeof Kind)[keyof typeof Kind];

/**
 * FrameInit is the partial shape callers pass to encodeFrame. All fields are
 * optional except the two that every frame carries (streamId, kind). Unset
 * fields take protobuf defaults (0 / "" / empty).
 */
export interface FrameInit {
  streamId: number;
  kind: Kind;
  method?: string;
  headers?: Record<string, string>;
  payload?: Uint8Array;
  status?: {
    code: number;
    message?: string;
    details?: Uint8Array[];
  };
}

/** encodeFrame marshals a FrameInit into the binary wire form of a Frame. */
export function encodeFrame(init: FrameInit): Uint8Array {
  const status =
    init.status === undefined
      ? undefined
      : create(StatusSchema, {
          code: init.status.code,
          message: init.status.message ?? "",
          details: init.status.details ?? [],
        });

  const frame = create(FrameSchema, {
    streamId: init.streamId,
    kind: init.kind,
    method: init.method ?? "",
    headers: init.headers ?? {},
    payload: init.payload ?? new Uint8Array(0),
    status,
  });
  return toBinary(FrameSchema, frame);
}

/**
 * decodeFrame unmarshals the binary wire form of a Frame.
 *
 * Note: inbound message size bounding is intentionally delegated to the
 * WebSocket layer and the server; no hard cap is applied here so that large
 * but legitimate payloads are not rejected by the client runtime.
 */
export function decodeFrame(bytes: Uint8Array): Frame {
  return fromBinary(FrameSchema, bytes);
}
