import { parseArgs } from "jsr:@std/cli/parse-args";
import { extractTextTerms } from "../textIndex.ts";
import * as path from "jsr:@std/path";
import { extractMelodyTerms } from "../melodyIndex.ts";
import { Resolver } from "../indexReader.ts";
import { SearchService } from "../search.ts";

function getConfig() {
  const args = parseArgs(Deno.args);
  if (!args.dbPath) {
    console.error("Supply --dbPath arg");
    Deno.exit(2);
  }

  return args;
}

class LocalFileResolver implements Resolver {
  constructor(public dbPath: string) {}

  // Number of network requests.
  private requests = 0;

  // Number of request bytes.
  private requestBytes = 0;

  async loadManifestManifestForType(indexType: string) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "manifest-manifest",
    );

    const fileBuf = await Deno.readFile(filePath);
    this.requests += 1;
    this.requestBytes += fileBuf.byteLength;

    // Take a copy of the buffer.
    return new BigUint64Array(new BigUint64Array(fileBuf.buffer));
  }

  async getManifestChunk(indexType: string, chunkId: number) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "manifest-" + chunkId,
    );
    const fileBuf = await Deno.readFile(filePath);
    this.requests += 1;
    this.requestBytes += fileBuf.byteLength;

    // Take a copy of the buffer.
    return new BigUint64Array(new BigUint64Array(fileBuf.buffer));
  }

  async getPageId(indexType: string, pageId: number) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "page-" + pageId,
    );
    const fileBuf = await Deno.readFile(filePath);
    this.requests += 1;
    this.requestBytes += fileBuf.byteLength;

    // Take a copy of the buffer.
    return new Uint32Array(new Uint32Array(fileBuf.buffer));
  }

  getTotalRequests(): number {
    return this.requests;
  }

  getTotalRequestBytes(): number {
    return this.requestBytes;
  }
}

async function run() {
  const config = getConfig();

  const resolver = new LocalFileResolver(config.dbPath);
  const search = new SearchService(resolver);
  search.initType("titleText");
  search.initType("melodyIndex");
  search.initType("melodyIncipitIndex");

  if (config.titleSearch) {
    const text = config.titleSearch;

    const terms = extractTextTerms([text]);

    const result = await search.search("titleText", terms);

    console.log(result);
  }

  if (config.melodySearch) {
    const text = config.melodySearch;
    if (typeof text == "string") {
      const numbers = text.split(",").map((x) => {
        return parseInt(x);
      });

      const terms = extractMelodyTerms(numbers);
      const result = await search.search("melodyIncipitIndex", terms);

      console.log(result);
    }
  }
}

run();
