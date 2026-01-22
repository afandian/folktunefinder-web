import { PagedIndexReaderFileDriver } from "./diskStorageDriver.ts";
import { PagedIndexReader } from "./index.ts";
import { PagedIndexReaderDriver } from "./types.ts";

// Service to perform search.

export class SearchResult {
  constructor(
    public result: Array<[number, number]>,
    public requests: number,
    public requestBytes: number,
  ) {}
}

// Composable query for terms on a specific index type.
export class IndexSearchQuery {
  constructor(
    public indexName: string,
    public terms: Array<[bigint, number]>,
  ) {}

  // Sort for more efficient retrieval.
  sort() {
    this.terms.sort(([aTerm, _aPos], [bTerm, _bPos]) => Number(aTerm - bTerm));
  }
}

// Service for searching indexes.
// Has a resolver which it uses to retrieve data.
export class SearchService {
  // Cache per index type.
  private caches = new Map<string, PagedIndexReader>();

  constructor(
    private driver: PagedIndexReaderDriver,
    private indexTypeNames: Array<string>,
    private pageSizeBytes: number,
  ) {
    for (const typeName of indexTypeNames) {
      this.caches.set(
        typeName,
        new PagedIndexReader(
          pageSizeBytes,
          driver,
          typeName,
        ),
      );
    }
  }

  async search(searchRequests: Array<IndexSearchQuery>) {
    const preRequests = this.driver.getRequests();
    const preBytes = this.driver.getBytes();

    // doc id to score
    const docIdScores = new Map<number, number>();

    for (const searchRequest of searchRequests) {
      const cache = this.caches.get(searchRequest.indexName);
      if (!cache) {
        console.error("Didn't find cache for ", searchRequest.indexName);
        return null;
      }

      searchRequest.sort();

      // Simply sum the results.
      for (const [term, _position] of searchRequest.terms) {
        const entries = await cache.fetchTermEntries(term);
        if (entries) {
          for (const [docId, _position] of entries) {
            const score = docIdScores.get(docId) || 0;
            docIdScores.set(docId, score + 1);
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
    const requests = this.driver.getRequests() - preRequests;
    const requestBytes = this.driver.getBytes() - preBytes;

    return new SearchResult(result, requests, requestBytes);
  }
}
