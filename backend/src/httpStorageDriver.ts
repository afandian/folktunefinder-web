// Driver for file access over HTTP

import { PagedIndexReaderDriver } from "./types.ts";

// Driver for storage and retrieval from local storage.

export class PagedIndexReaderHttpDriver implements PagedIndexReaderDriver {
  public totalRequests: number = 0;
  public totalBytes: number = 0;
  constructor(
    private baseUrl: string,
  ) {
  }

  getRequests() {
    return this.totalRequests;
  }
  getBytes() {
    return this.totalBytes;
  }

  async readPage(
    indexTypeName: string,
    pageNumber: number,
  ) {
    const filePath =
      `${this.baseUrl}/index/${indexTypeName}/page-${pageNumber}`;
    const responseBuffer = await (await fetch(filePath)).arrayBuffer();
    this.totalRequests += 1;

    this.totalBytes += responseBuffer.byteLength;
    return new Uint32Array(responseBuffer);
  }

  async readPageTable(indexTypeName: string) {
    const filePath = `${this.baseUrl}/index/${indexTypeName}/page-table`;
    const responseBuffer = await (await fetch(filePath)).arrayBuffer();

    this.totalRequests += 1;
    this.totalBytes += responseBuffer.byteLength;
    return new BigUint64Array(responseBuffer);
  }
}
