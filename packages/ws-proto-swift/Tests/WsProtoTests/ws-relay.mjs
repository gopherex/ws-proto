// Linux-only test socket: preserves WebSocket message boundaries while Swift
// runs the actual ws-proto mux, protobuf codecs, lifecycle, and flow control.
import WebSocket from "ws";
import { createInterface } from "node:readline";

const socket = new WebSocket(process.argv[2], "wsrpc.v1");
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
socket.on("open", () => emit({ open: socket.protocol }));
socket.on("message", (data, binary) => {
  if (binary) emit({ data: data.toString("base64") });
});
socket.on("error", () => emit({ closed: true }));
socket.on("close", () => {
  emit({ closed: true });
  process.exitCode = 0;
  process.stdin.destroy();
});
const input = createInterface({ input: process.stdin });
input.on("line", (line) => {
  if (socket.readyState === WebSocket.OPEN) socket.send(Buffer.from(line, "base64"));
});
input.on("close", () => socket.close());
