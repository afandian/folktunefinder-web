import { parseArgs } from "jsr:@std/cli/parse-args";
import { extractTextTerms } from "../textIndex.ts";
import * as path from "jsr:@std/path";

function getConfig() {
  const args = parseArgs(Deno.args);
  if (!args.dbPath) {
    console.error("Supply --dbPath arg");
    Deno.exit(2);
  }

  return args;
}

class LocalFileResolver {
  constructor(public dbPath: string) {}

  // Number of network requests.
  public requests = 0;

  // Number of request bytes.
  public requestBytes = 0;

  loadManifestManifestForType(indexType: string) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "manifest-manifest",
    );

    return Deno.readFile(filePath).then((fileBuf) => {
      this.requests += 1;
      this.requestBytes += fileBuf.byteLength;

      // Take a copy of the buffer.
      return new BigUint64Array(new BigUint64Array(fileBuf.buffer));
    });
  }

  getManifestChunk(indexType: string, chunkId: number) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "manifest-" + chunkId,
    );
    return Deno.readFile(filePath).then((fileBuf) => {
      this.requests += 1;
      this.requestBytes += fileBuf.byteLength;

      // Take a copy of the buffer.
      return new BigUint64Array(new BigUint64Array(fileBuf.buffer));
    });
  }

  getPageId(indexType: string, pageId: number) {
    const filePath = path.join(
      this.dbPath,
      "index",
      indexType,
      "page-" + pageId,
    );
    return Deno.readFile(filePath).then((fileBuf) => {
      this.requests += 1;
      this.requestBytes += fileBuf.byteLength;

      // Take a copy of the buffer.
      return new Uint32Array(new Uint32Array(fileBuf.buffer));
    });
  }
}

// Caching index loader.
// Promises are used both for async, and to indicate that retrieval is in progress.
class IndexCache {
  constructor(public indexType: string, public resolver: LocalFileResolver) {}

  // List of [term, manifest chunk file id]
  private manifestManifest: Promise<BigUint64Array> | null = null;

  // Loaded manifest Chunks. Signifies which ones have been loaded.
  // Data is loaded into termPageMap.
  private manifestChunkLoaded = new Map<number, boolean>();

  // Map of term to [page id, offset, length] loaded from manifest chunks.
  private termPageMap = new Map<bigint, [number, number, number]>();

  // Map of page ID to page.
  private termOccurrencePages = new Map<number, Promise<Uint32Array>>();

  init() {
    this.loadManifestManifest();
  }

  // Load manifestManifest in the backgroud.
  loadManifestManifest() {
    // Already fetched it (or started);
    if (this.manifestManifest) {
      return;
    }

    this.manifestManifest = this.resolver.loadManifestManifestForType(
      this.indexType,
    );
  }

  // Load a page
  getPageId(pageId: number) {
    const found = this.termOccurrencePages.get(pageId);
    if (found) {
      return found;
    }

    const page = this.resolver.getPageId(this.indexType, pageId);
    this.termOccurrencePages.set(pageId, page);
    return page;
  }

  // Retrieve the relevant manifest chunk for the term.
  async getManifestChunkIdForTerm(term: bigint) {
    // Ensure loaded first time.
    this.loadManifestManifest();

    if (!this.manifestManifest) {
      console.error("Can't load manifestManifest");
      return;
    }

    const manifestManifest = await this.manifestManifest;

    for (let i = 0; i <= manifestManifest.length; i += 3) {
      const firstTerm = manifestManifest[i];
      const lastTerm = manifestManifest[i + 1];
      const manifestChunkId = manifestManifest[i + 2];

      if (term >= firstTerm && term <= lastTerm) {
        // Stored in the file as a UInt64 but it's a number.
        return Number(manifestChunkId);
      }
    }
  }

  // Ensure that the data from the given manifest chunk is loaded.
  // Result will be stored in termPageMap .
  // Returns promise when the data is loaded.
  loadManifestChunk(manifestChunkId: number): Promise<boolean> {
    if (this.manifestChunkLoaded.get(manifestChunkId)) {
      return new Promise((resolve, _) => {
        resolve(true);
      });
    }

    return this.resolver.getManifestChunk(this.indexType, manifestChunkId).then(
      (buf) => {
        const v = new DataView(buf.buffer);
        for (let o = 0; o < buf.buffer.byteLength;) {
          const term = v.getBigUint64(o, true);
          o += 8;

          const page = v.getUint32(o, true);
          o += 4;

          const offset = v.getUint32(o, true);
          o += 4;
          const length = v.getUint32(o, true);
          o += 4;
          // spare
          o += 4;

          this.termPageMap.set(term, [page, offset, length]);
        }

        return true;
      },
    );
  }

  async getEntryForTerm(term: bigint) {
    const manifestChunkId = await this.getManifestChunkIdForTerm(term);
    if (!manifestChunkId) {
      // Not an error, just means searching for unknown term.
      console.log("Can't get chunk for term", term, "in index", this.indexType);
      return null;
    }
    // Ensure that the cache has termPageMap populated for this chunk.
    return this.loadManifestChunk(manifestChunkId).then(async () => {
      const termPageMapEntry = this.termPageMap.get(term);
      if (!termPageMapEntry) {
        // This is not an error, it just means the term wasn't found.
        console.info("Can't find term page map entry for term", term);
        return null;
      }
      const [pageId, offset, length] = termPageMapEntry;
      const page = await this.getPageId(pageId);

      if (page) {
        return page.subarray(offset, offset + length);
      }
    });
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
      (accumulator, [_type, cache]) => accumulator + cache.resolver.requests,
      0,
    );
  }

  totalRequestBytes() {
    return this.caches.entries().reduce(
      (accumulator, [_type, cache]) =>
        accumulator + cache.resolver.requestBytes,
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
}

run();
