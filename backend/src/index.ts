import {
  Address,
  BYTES_PER_DOC_OCCURRENCE,
  DocOccurrenceList,
  DocTermOccurences,
  PagedIndexReaderDriver,
  Term,
  TermDocOccurrences,
} from "./types.ts";
import { PagedIndexWriterFileDriver } from "./diskStorageDriver.ts";

// Read and write indexes.

export function generateTermDocIndex(
  docTermOccurrences: DocTermOccurences,
): TermDocOccurrences {
  // map of term to doc and position
  const result = new Map<bigint, Array<[number, number]>>();
  for (const [docId, termPositions] of docTermOccurrences.docTermOccurrence) {
    for (const [term, position] of termPositions) {
      let entry = result.get(term);
      if (!entry) {
        entry = [];
        result.set(term, entry);
      }
      entry.push([docId, position]);
    }
  }

  console.log("Finished generate term doc occurrences");

  return result;
}

// Paged index writer.
// Stateful, single-threaded, and expects monotonic write-only access.
// Expects terms to be added monotonically.
// Represents a mapping of bigint terms to Document Occurrences.
export class PagedIndexWriter {
  constructor(
    // Size of a page in bytes.
    pageSizeByes: number,
    private driver: PagedIndexWriterFileDriver,
    public indexTypeName: string,
  ) {
    if (pageSizeByes % BYTES_PER_DOC_OCCURRENCE != 0) {
      throw "Page size must be a multiple of BYTES_PER_DOC_OCCURRENCE";
    }

    if (pageSizeByes <= 0) {
      throw "Page size must be greater than zero";
    }

    this.pageSize = pageSizeByes / BYTES_PER_DOC_OCCURRENCE;
    this.buffer = new Uint32Array(this.pageSize);
    this.bufferByteView = new Uint8Array(this.buffer.buffer);
  }

  // Buffer for current page.
  buffer: Uint32Array;

  // Bytes view on buffer.
  bufferByteView: Uint8Array;

  // Page size in words.
  pageSize: Address;

  // uint32 current offset in the whole address space.
  // currentOffset: Address = 0;

  currentPageNumber: number = 0;

  currentOffsetInPage: Address = 0;

  previousTerm = -1n;

  // Table mapping the first term found in each page.
  // Becaues the terms are sorted, this forms a skiplist.
  pageTable = new Map<number, Term>();

  termWrittenInPage = false;

  async start() {
    // Write the first 'expect term'.
    this.writeWord(0x00000000);
  }

  // Flush this page and get buffer ready for next page.
  async nextPage() {
    await this.driver.writePage(
      this.indexTypeName,
      this.currentPageNumber,
      this.bufferByteView,
    );
    this.buffer.fill(0xFFFFFFFF);
    this.currentPageNumber += 1;

    this.termWrittenInPage = false;
    this.currentOffsetInPage = 0;
  }

  async write(termDocOccurrences: TermDocOccurrences) {
    console.log("Write term occurrences for", this.indexTypeName, "...");
    const terms = BigUint64Array.from(termDocOccurrences.keys());
    terms.sort();

    for (const term of terms) {
      const occurrences = termDocOccurrences.get(term);

      if (occurrences) {
        await this.writeForTerm(term, occurrences);
      }
    }

    this.writeTerminate();

    // Flush the last page.
    await this.nextPage();

    await this.writePageTable();

    console.log("Finished writing term occurrences!");
  }

  // Write uint32
  async writeWord(word: number) {
    if (this.currentOffsetInPage >= this.pageSize) {
      await this.nextPage();
    }
    this.buffer[this.currentOffsetInPage] = word;
    this.currentOffsetInPage += 1;
  }

  async writeTerminate() {
    await this.writeWord(0xFFFFFFFF);
    await this.writeWord(0xFFFFFFFF);
  }

  async writeForTerm(term: Term, entries: DocOccurrenceList) {
    if (term == 0xFFFFFFFFFFFFFFFFn) {
      console.error("Reserved term!", term, entries);
      return;
    }

    // Ensure that all terms are sorted and can't be inserted out of order.
    if (term <= this.previousTerm) {
      throw `Term ${term} should be greater than ${this.previousTerm}`;
    }
    this.previousTerm = term;

    // Write entries into 32 bit words.
    // Space for:
    //  - 8 byte term (2 x 32 bits),
    //  - variable length sequence (1 or more x 32 bits)
    //  - 4 byte (1 x 32 bits) terminating null.
    // Minimum 4 bytes.

    // Save both 32-bit halves of the 64-bit term.
    await this.writeWord(Number(term >> 32n));
    await this.writeWord(Number(term & 0xFFFFFFFFn));

    // First term written in a page is stored in page table.
    if (!this.termWrittenInPage) {
      this.pageTable.set(this.currentPageNumber, term);
      this.termWrittenInPage = true;
    }

    for (const [docId, position] of entries) {
      const docIdPart = docId & 0x00FFFFFF;
      const positionPart = (position & 0xFF) << 24;
      const tagged = docIdPart | positionPart;

      await this.writeWord(tagged);
    }

    // Write terminating null to mark end of the sequence.
    await this.writeWord(0x00000000);
  }

  async writePageTable() {
    const buf = new BigUint64Array(this.pageTable.size * 2);
    const byteView = new Uint8Array(buf.buffer);
    let i = 0;
    for (const [pageId, number] of this.pageTable.entries()) {
      buf[i++] = BigInt(pageId);
      buf[i++] = number;
    }

    await this.driver.writePageTable(this.indexTypeName, byteView);
  }

  flush() {
  }
}

// Paged index writer.
// Stateful, single-threaded, and expects monotonic write-only access.
// Expects terms to be added monotonically.
// Represents a mapping of bigint terms to Document Occurrences.
export class PagedIndexReader {
  constructor(
    // Size of a page in bytes.
    pageSizeBytes: number,
    private driver: PagedIndexReaderDriver,
    public indexTypeName: string,
  ) {
    if (pageSizeBytes % BYTES_PER_DOC_OCCURRENCE != 0) {
      throw "Page size must be a multiple of BYTES_PER_DOC_OCCURRENCE";
    }

    if (pageSizeBytes <= 0) {
      throw "Page size must be greater than zero";
    }

    this.pageSize = pageSizeBytes / BYTES_PER_DOC_OCCURRENCE;

    // Fetch page table in background.
    this.pageTable = this.driver.readPageTable(this.indexTypeName);
  }

  pageSize: number;
  currentPageId: number = 0;
  currentAddress: Address = 0;
  currentAddressInPage: Address = 0;
  currentPage: Uint32Array | null = null;

  // Page table. Sequences of pairs of [page id, first term]
  // Terms are sorted across pages.
  pageTable: Promise<BigUint64Array>;

  // Cache of retrieved pages.
  pages = new Map<number, Uint32Array>();

  async pageIdForTerm(term: Term) {
    const pageTable = await this.pageTable;

    // Will remain null if we never found a page that contained this term.
    let prevPage = null;
    // Pages contain terms stored in order.
    for (let i = 0; i < pageTable.length; i++) {
      // TODO assert that terms are in order
      const pageId = Number(pageTable[i * 2]);
      const firstTerm = pageTable[(i * 2) + 1];

      // Scan whilst the page doesn't yet contain the term.
      if (firstTerm <= term) {
        prevPage = pageId;
      } else if (firstTerm > term) {
        // Return the previous page, which did contain the term.
        // Or null if none did.
        return prevPage;
      }
    }
    return null;
  }

  async fetchPage(pageId: number) {
    const page = this.pages.get(pageId);
    if (page) {
      return page;
    }

    // TODO handle missing file.
    const newPage = await this.driver.readPage(this.indexTypeName, pageId);
    this.pages.set(pageId, newPage);
    return newPage;
  }

  async jump(address: Address) {
    this.currentAddress = address;
    this.currentPageId = Math.floor(address / this.pageSize);
    this.currentAddressInPage = address % this.pageSize;
    this.currentPage = await this.fetchPage(this.currentPageId);
  }

  async readU32() {
    if (!this.currentPage) {
      throw "No Page";
    }

    if (this.currentAddressInPage >= this.currentPage.length) {
      throw "Too big";
    }

    const value = this.currentPage[this.currentAddressInPage];

    await this.jump(this.currentAddress + 1);

    return value;
  }

  async readTerm() {
    const high = await this.readU32();
    const low = await this.readU32();
    const term = BigInt(high) << 32n | BigInt(low);
    return term;
  }

  // Fast-forward to the next term marker.
  async findNextTerm() {
    while (true) {
      const value = await this.readU32();
      if (value == 0x00000000) {
        return true;
      }
    }
  }

  async readEntryList(): Promise<DocOccurrenceList> {
    const result: DocOccurrenceList = [];
    for (
      let entry = await this.readU32();
      entry != 0x00;
      entry = await this.readU32()
    ) {
      if (entry == undefined) {
        console.log("UNDEFINED");
        break;
      }

      const docIdPart = entry & 0x00FFFFFF;
      const positionPart = entry >>> 24;

      result.push([docIdPart, positionPart]);
    }

    return result;
  }

  async *allEntries() {
    await this.jump(0);
    yield* this.scan();
  }

  async *scan(): AsyncGenerator<
    [Term, DocOccurrenceList],
    void,
    unknown
  > {
    try {
      while (this.currentPage != null) {
        // We jumped mid-page, so fast-forward to the first term in the page.
        await this.findNextTerm();

        const term = await this.readTerm();

        // console.log("GOT TERM", term);
        if (term == 0xFFFFFFFFFFFFFFFFn) {
          console.log("End");
          break;
        }
        const entryList = await this.readEntryList();

        if (term && entryList) {
          yield [term, entryList];
        }
      }
    } catch (e) {
      console.error("EE", e);
    }
  }

  async fetchTermEntries(term: Term) {
    const pageId = await this.pageIdForTerm(term);

    if (!pageId) {
      // Term not found in index.
      return null;
    }

    const startAddress = pageId * this.pageSize;

    await this.jump(startAddress);

    for await (const [thisTerm, entries] of this.scan()) {
      if (thisTerm == term) {
        return entries;
      }
      // Iterate in order. Break when exceeded term.
      if (thisTerm > term) {
        return null;
      }
    }
    return null;
  }
}
