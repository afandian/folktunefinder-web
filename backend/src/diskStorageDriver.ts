import * as path from "jsr:@std/path";
import { PagedIndexReaderDriver } from "./types.ts";

// Driver for storage and retrieval from local storage.

export class PagedIndexWriterFileDriver {
  constructor(
    private dbPath: string,
  ) {
  }

  async writePage(
    indexTypeName: string,
    pageNumber: number,
    buffer: Uint8Array,
  ) {
    const dirPath = path.join(this.dbPath, "index", indexTypeName);
    await Deno.mkdir(dirPath, { recursive: true });
    const filePath = path.join(dirPath, `page-${pageNumber}`);
    console.log("Write ", filePath, "...");

    await Deno.writeFile(filePath, buffer);
  }

  async writePageTable(indexTypeName: string, buffer: Uint8Array) {
    const dirPath = path.join(this.dbPath, "index", indexTypeName);
    await Deno.mkdir(dirPath, { recursive: true });
    const filePath = path.join(dirPath, "page-table");
    console.log("Write ", filePath, "...");

    await Deno.writeFile(filePath, buffer);
  }
}

export class PagedIndexReaderFileDriver implements PagedIndexReaderDriver {
  public totalRequests: number = 0;
  public totalBytes: number = 0;
  constructor(
    private dbPath: string,
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
  ): Promise<Uint32Array> {
    const dirPath = path.join(this.dbPath, "index", indexTypeName);
    await Deno.mkdir(dirPath, { recursive: true });
    const filePath = path.join(dirPath, `page-${pageNumber}`);

    const result = await Deno.readFile(filePath);
    this.totalRequests += 1;

    this.totalBytes += result.length;

    return new Uint32Array(result.buffer);
  }

  async readPageTable(indexTypeName: string): Promise<BigUint64Array> {
    const dirPath = path.join(this.dbPath, "index", indexTypeName);
    await Deno.mkdir(dirPath, { recursive: true });
    const filePath = path.join(dirPath, "page-table");

    const result = await Deno.readFile(filePath);

    this.totalRequests += 1;
    this.totalBytes += result.length;

    return new BigUint64Array(result.buffer);
  }
}
