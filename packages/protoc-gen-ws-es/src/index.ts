import { createEcmaScriptPlugin, runNodeJs } from "@bufbuild/protoplugin";
import { generateTs } from "./ws-es.js";

export const protocGenWsEs = createEcmaScriptPlugin({
  name: "protoc-gen-ws-es",
  version: "v0.1.0",
  // We only emit TypeScript; protoc-gen-es itself handles js/dts of messages.
  generateTs,
});

runNodeJs(protocGenWsEs);
