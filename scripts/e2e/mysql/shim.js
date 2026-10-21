import * as __events from "node:events";
import * as __util from "node:util";
import * as __net from "node:net";
import * as __tls from "node:tls";
import * as __crypto from "node:crypto";
import * as __stream from "node:stream";
import * as __string_decoder from "node:string_decoder";
import * as __url from "node:url";
import * as __zlib from "node:zlib";
import * as __process from "node:process";
import * as __buffer from "node:buffer";
import * as __timers from "node:timers";
import * as __fs from "node:fs";
import * as __path from "node:path";
import * as __os from "node:os";
const __mods = {
  events: __events, util: __util, net: __net, tls: __tls, crypto: __crypto,
  stream: __stream, string_decoder: __string_decoder, url: __url, zlib: __zlib,
  process: __process, buffer: { ...__buffer }, timers: __timers, fs: __fs,
  path: __path, os: __os,
};
globalThis.require = (name) => {
  const key = String(name).replace(/^node:/, "");
  if (__mods[key]) return __mods[key];
  throw new Error("unsupported require: " + name);
};
globalThis.__dirname = "/";
globalThis.__filename = "/worker.js";
