import * as path from "jsr:@std/path";

function getLongestEntryLength(input: Map<bigint, Array<number>>) {
  let longest = 0;
  for (const [_k, vs] of input) {
    if (vs.length > longest) {
      longest = vs.length;
    }
  }

  return longest;
}

function getTotalEntriesLength(input: Map<bigint, Array<number>>) {
  let total = 1000;
  for (const [_, vs] of input) {
    total += vs.length;
  }

  return total;
}

export class TermDocIndex {
  constructor(
    public typeName: string,
    public termDocIndex: Map<bigint, Array<number>>,
  ) {}
}

/// For an input term doc index, return a sorted list of terms, most popular first.
function generateTermsPopularity(termDocIndex: Map<bigint, number[]>) {
  // Terms in descending order of popularity.
  // This puts more popular terms on lower pages.
  const termsPopularity = new Array<[bigint, number]>();
  for (const [term, entries] of termDocIndex) {
    termsPopularity.push([term, entries.length]);
  }
  termsPopularity.sort(([_termA, countA], [_termB, countB]) => {
    return countB - countA;
  });

  return termsPopularity;
}

// Write out pages and manifest for index.
export async function outputPages(
  dbPath: string,
  inputIndex: TermDocIndex,
) {
  // Page size. This isn't a hard limit, and can be exceeded if a single term doesn't fit.
  // Otherwise, try to pack these as full as possible.
  // This is an abirtrary choice, to be tuned based on characteristics.
  // Stop words play a role in making sure that indiscriminate words (like 'the') are removed.
  const pageSize = 65536;

  const pagesDir = path.join(dbPath, "index", inputIndex.typeName);
  Deno.mkdirSync(pagesDir, { recursive: true });

  console.log("Write index of type", inputIndex.typeName, "to", pagesDir);
  const longestEntryLength = getLongestEntryLength(inputIndex.termDocIndex);
  const totalEntryLength = getTotalEntriesLength(inputIndex.termDocIndex);

  console.log("Longest entry: ", longestEntryLength);
  console.log("Number of terms: ", inputIndex.termDocIndex.size);
  console.log("Total entry length: ", totalEntryLength);

  // Get list of terms in order of popularity.
  const termsPopularity = generateTermsPopularity(inputIndex.termDocIndex);

  // Keep track of terms that were already written to an index.
  // This enables probing for page-packing.
  const writtenTerms = new Set<bigint>();

  // Mapping of index term to page number, u32 page, offset as a 32-bit word offset, length as 32-bit word.
  const manifest = new Array<[bigint, number, number, number]>();

  // Current page ID.
  let pageId = 0;

  // Page buffer. This is re-used.
  // Layout of page buffer:
  // Docs: u32 * - 4 bytes
  // The term is not stored in the page, just the entry list.
  const pageBuffer = new Uint32Array(pageSize / 4);

  // For file writing.
  const uint8view = new Uint8Array(pageBuffer.buffer);

  // Number of 32 bit words used in the buffer.
  let pageUsed = 0;

  // Number of entries we put in the page.
  let pageEntryCout = 0;

  // Descend the term popularity list and pack the page with the most popular available.
  for (let i = 0; i < termsPopularity.length; i++) {
    // Starting at this term i, probe for entries that can fill the page.
    for (let j = i; j < termsPopularity.length; j++) {
      // Get this term.
      const [term, _] = termsPopularity[i];

      // Keep track of terms that have been already used to pack a page.
      if (writtenTerms.has(term)) {
        continue;
      } else {
        writtenTerms.add(term);
      }

      const termEntries = inputIndex.termDocIndex.get(term);

      if (!termEntries) continue;

      // Bytes required to store this term's worth of data.
      const termRequired = termEntries.length * 4;

      const anySpace = (pageUsed * 4 + termRequired) < pageSize;

      if (anySpace) {
        manifest.push([term, pageId, pageUsed, termEntries.length]);
        for (let z = 0; z < termEntries.length; z++) {
          pageBuffer[pageUsed + z] = termEntries[z];
        }
        pageUsed += termEntries.length;
        pageEntryCout += 1;
      } else {
        // If there wasn't any space for this term in the page, and this was
        // the first one in the page, then it won't fit on any, so don't probe.
        // Instead write an emergency oversized page.
        // It's OK to have a handful of these. But not worth expanding page size for the majority of the index.
        // If this happens often, then the page size should be increased.
        // However, if this happens
        if (pageUsed == 0) {
          console.error(
            "Term ",
            term,
            " requires ",
            termRequired,
            " bytes, too much for page size ",
            pageSize,
            ". Increase page size!",
          );

          const oversizedPageBuffer = new Uint32Array(termRequired);
          // For file writing.
          const oversizedPageBufferUint8View = new Uint8Array(
            oversizedPageBuffer.buffer,
          );

          manifest.push([term, pageId, pageUsed, termEntries.length]);
          for (let z = 0; z < termEntries.length; z++) {
            oversizedPageBuffer[pageUsed + z] = termEntries[z];
          }

          const pagePath = path.join(pagesDir, "page-" + pageId);
          console.log("Save oversized index page to ", pagePath);

          await Deno.writeFile(pagePath, oversizedPageBufferUint8View);

          pageId += 1;
          pageBuffer.fill(0);
          pageUsed = 0;
          pageEntryCout = 0;
        } else {
          const fill = (pageUsed * 4) / pageSize;

          // No more space in the page.
          const pagePath = path.join(pagesDir, "page-" + pageId);
          console.log(
            "Save index page to ",
            pagePath,
            "with",
            pageEntryCout,
            "entries, fill factor",
            fill,
          );

          await Deno.writeFile(pagePath, uint8view);

          pageId += 1;
          pageBuffer.fill(0);
          pageUsed = 0;
          pageEntryCout = 0;
        }
      }
    }
  }

  const manifestBytes = manifest.length * (8 + 4 + 4 + 4 + 4);
  console.log(
    "Write manifest of",
    manifest.length,
    "terms in ",
    manifestBytes,
    "bytes",
  );

  // Structure of manifest:
  // term : u64
  // page: u32
  // offset: u32
  // length: u32
  // padding: u32
  // Stride of 24 bytes. Aligned to 64 bits.
  const manifestBuf = new ArrayBuffer(manifestBytes);
  const manifestView = new DataView(manifestBuf);
  const manifest8View = new Uint8Array(manifestBuf);

  manifest.sort(
    (
      [termA, _pageA, _offsetA, _lengthA],
      [termB, _pageB, _offsetB, _lengthB],
    ) => {
      if (termA == termB) return 0;
      else if (termA > termB) return 1;
      else return -1;
    },
  );

  let o = 0;
  for (const [term, page, offset, length] of manifest) {
    manifestView.setBigUint64(o, term, true);
    o += 8;
    manifestView.setInt32(o, page, true);
    o += 4;
    manifestView.setInt32(o, offset, true);
    o += 4;
    manifestView.setInt32(o, length, true);
    o += 4;
    // padding
    o += 4;
  }

  const manifestPath = path.join(pagesDir, "manifest");
  await Deno.writeFile(manifestPath, manifest8View);

  console.log("Done");
}
