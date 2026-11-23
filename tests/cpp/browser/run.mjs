import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { once } from "node:events";

const upstream = new URL(process.env.WS_PROTO_TEST_URL);
const dir = process.argv[2];
const profile = await mkdtemp(path.join(os.tmpdir(), "ws-proto-firefox-"));
const { promise, resolve, reject } = Promise.withResolvers();
const assets = new Map([["/", "index.html"], ["/echo.js", "echo.js"], ["/echo.wasm", "echo.wasm"]]);
const server = http.createServer(async (req, res) => {
  if (req.url === "/result" && req.method === "POST") {
    let result = "";
    for await (const part of req) result += part;
    res.end();
    resolve(result);
    return;
  }
  if (req.url === "/cancelled") {
    const probe = new URL("/probe?id=cpp-browser", upstream);
    probe.protocol = "http:";
    const signal = AbortSignal.timeout(5000);
    try {
      for (;;) {
        const response = await fetch(probe, { signal });
        const state = await response.json();
        if (state.finished) {
          assert.ok(state.started && state.cancelled, "Go handler did not observe RPC cancellation");
          break;
        }
        await delay(10, undefined, { signal });
      }
      res.writeHead(204).end();
    } catch (err) { res.writeHead(500).end(); reject(err); }
    return;
  }
  const file = assets.get(req.url);
  if (!file) { res.writeHead(404).end(); return; }
  try {
    const body = await readFile(path.join(dir, file));
    const type = file.endsWith(".wasm") ? "application/wasm" : file.endsWith(".js") ? "text/javascript" : "text/html";
    res.writeHead(200, { "content-type": type }).end(body);
  } catch (err) { res.writeHead(500).end(); reject(err); }
});
// Preserve Host and Origin through the loopback tunnel for the Go same-origin check.
server.on("upgrade", (req, socket, head) => {
  const peer = net.connect(Number(upstream.port), upstream.hostname, () => {
    peer.write("GET / HTTP/1.1\r\n");
    for (let i = 0; i < req.rawHeaders.length; i += 2)
      peer.write(req.rawHeaders[i] + ": " + req.rawHeaders[i + 1] + "\r\n");
    peer.write("\r\n");
    peer.write(head);
    socket.pipe(peer).pipe(socket);
  });
  socket.on("error", () => peer.destroy());
  peer.on("error", err => { socket.destroy(); reject(err); });
  socket.on("close", () => peer.destroy());
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const browser = spawn(process.env.FIREFOX || "firefox", ["--headless", "--no-remote", "--profile", profile,
  "http://127.0.0.1:" + server.address().port], { stdio: "ignore" });
const exit = once(browser, "exit");
exit.then(([code]) => reject(Error("Firefox exited: " + code)), reject);
const timeout = setTimeout(() => reject(Error("browser RPC test timed out")), 60000);
try {
  assert.equal(await promise, "ok");
  console.log("WASM / Firefox / Go: all four RPC kinds passed");
} finally {
  clearTimeout(timeout);
  browser.kill();
  await exit.catch(() => {});
  server.closeAllConnections();
  server.close();
  await rm(profile, { recursive: true, force: true });
}
