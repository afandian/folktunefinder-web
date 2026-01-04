import { IndexCache, Resolver } from "./indexReader.ts";

export class SearchResult {
  constructor(
    public result: Array<[number, number]>,
    public requests: number,
    public requestBytes: number,
  ) {}
}

// Composable query for terms on a specific index type.
export class IndexSearchQuery {
  constructor(public indexName: string, public terms: bigint[]) {}
}

// Service for searching indexes.
// Has a resolver which it uses to retrieve data.
export class SearchService {
  // Cache per index type.
  private caches = new Map<string, IndexCache>();

  constructor(private resolver: Resolver) {
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

  async search(searchRequests: Array<IndexSearchQuery>) {
    const preRequests = this.totalRequests();
    const preRequestBytes = this.totalRequestBytes();

    // doc id to score
    const docIdScores = new Map<number, number>();

    // Pre-fetch all types requested.
    for (const searchRequest of searchRequests) {
      // Ensure the cache is loaded for this type.
      this.initType(searchRequest.indexName);
    }

    for (const searchRequest of searchRequests) {
      const cache = this.caches.get(searchRequest.indexName);
      if (!cache) {
        console.error("Didn't find cache for ", searchRequest.indexName);
        return null;
      }

      for (const term of searchRequest.terms) {
        const entries = await cache.getEntryForTerm(term);
        if (entries) {
          for (const entry of entries) {
            const score = docIdScores.get(entry) || 0;
            docIdScores.set(entry, score + 1);
          }
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
