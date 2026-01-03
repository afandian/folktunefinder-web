import { parseArgs } from "jsr:@std/cli/parse-args";
import { extractTextTerms } from "../textIndex.ts";
import * as path from "jsr:@std/path";
import { extractMelodyTerms } from "../melodyIndex.ts";
import { IndexCache, Resolver } from "../indexReader.ts";

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

class SearchResult {
  constructor(
    public result: Array<[number, number]>,
    public requests: number,
    public requestBytes: number,
  ) {}
}

// Service for searching indexes.
// Has a resolver which it uses to retrieve data.
class SearchService {
  // Cache per index type.
  private caches = new Map<string, IndexCache>();

  constructor(private resolver: LocalFileResolver) {
  }

  // Instantiate the cache for this index type. Optional.
  // Allows for eager loading of the manifests in the background.
  initType(indexTypeName: string) {
    if (this.caches.get(indexTypeName)) {
      return;
    }

    const indexCache = new IndexCache(indexTypeName, this.resolver);
    indexCache.init();
    this.caches.set(indexTypeName, indexCache);
  }

  totalRequests() {
    return this.caches.entries().reduce(
      (accumulator, [_type, cache]) =>
        accumulator + cache.resolver.getTotalRequests(),
      0,
    );
  }

  totalRequestBytes() {
    return this.caches.entries().reduce(
      (accumulator, [_type, cache]) =>
        accumulator + cache.resolver.getTotalRequestBytes(),
      0,
    );
  }

  async search(indexTypeName: string, terms: bigint[]) {
    const preRequests = this.totalRequests();
    const preRequestBytes = this.totalRequestBytes();

    // Ensure the cache is loaded for this type.
    this.initType(indexTypeName);

    const cache = this.caches.get(indexTypeName);
    if (!cache) {
      console.error("Didn't find cache for ", indexTypeName);
      return null;
    }

    // doc id to score
    const docIdScores = new Map<number, number>();

    for (const term of terms) {
      const entries = await cache.getEntryForTerm(term);
      if (entries) {
        for (const entry of entries) {
          const score = docIdScores.get(entry) || 0;
          docIdScores.set(entry, score + 1);
        }
      }
    }

    const result = Array.from(docIdScores.entries());

    // Sort by score descending, and then by ID ascending.
    result.sort(
      (
        [docIdA, scoreA],
        [docIdB, scoreB],
      ) => {
        if (scoreA == scoreB) return (docIdA - docIdB);
        else return (scoreB - scoreA);
      },
    );

    // This isn't totally accurate as it records changes within the execution of this function,
    // which may include other background activity.
    const requests = this.totalRequests() - preRequests;
    const requestBytes = this.totalRequestBytes() - preRequestBytes;

    return new SearchResult(result, requests, requestBytes);
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
