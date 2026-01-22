// A uint64 term.
export type Term = bigint;

// a uint24 document id.
export type DocId = number;

// a uint8 position.
export type Position = number;

// List of doc and position in doc.
export type DocOccurrenceList = Array<[DocId, Position]>;

export type TermDocOccurrences = Map<Term, Array<[DocId, Position]>>;

// TODO COMBINE
export type DocTermOccurrences = Map<DocId, Array<[Term, Position]>>;

export class DocTermOccurences {
  constructor(
    public indexTypeName: string,
    // Doc ID => [term, position in doc]
    public docTermOccurrence: Map<number, Array<[bigint, number]>>,
  ) {}
}

// A position in the 32 bit address space.
// A word index (not bytes).
export type Address = number;

export const BYTES_PER_DOC_OCCURRENCE = 4;

export interface PagedIndexReaderDriver {
  readPage(
    indexTypeName: string,
    pageNumber: number,
  ): Promise<Uint32Array>;

  readPageTable(indexTypeName: string): Promise<BigUint64Array>;

  getRequests(): number;
  getBytes(): number;
}
