export interface Resolver {
  loadManifestManifestForType(
    indexType: string,
  ): Promise<BigUint64Array<ArrayBuffer>>;
  getManifestChunk(
    indexType: string,
    chunkId: number,
  ): Promise<BigUint64Array<ArrayBuffer>>;
  getPageId(
    indexType: string,
    pageId: number,
  ): Promise<Uint32Array<ArrayBuffer>>;

  getTotalRequests(): number;
  getTotalRequestBytes(): number;
}

// Caching index loader.
// Promises are used both for async, and to indicate that retrieval is in progress.
export class IndexCache {
  constructor(public indexType: string, public resolver: Resolver) {}

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
  async loadManifestChunk(manifestChunkId: number): Promise<boolean> {
    if (this.manifestChunkLoaded.get(manifestChunkId)) {
      return new Promise((resolve, _) => {
        resolve(true);
      });
    }

    const buf = await this.resolver.getManifestChunk(
      this.indexType,
      manifestChunkId,
    );
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
