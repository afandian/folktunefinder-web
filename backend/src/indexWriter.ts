import * as path from "jsr:@std/path";

// List of doc and position in doc.
type DocOccurrenceList = Array<[number, number]>;

type TermDocOccurrences = Map<bigint, Array<[number, number]>>;

function getLongestEntryLength(
  input: TermDocOccurrences,
) {
  let longest = 0;
  for (const [_k, vs] of input) {
    if (vs.length > longest) {
      longest = vs.length;
    }
  }

  return longest;
}

function getTotalEntriesLength(input: TermDocOccurrences) {
  let total = 1000;
  for (const [_, vs] of input) {
    total += vs.length;
  }

  return total;
}

export class DocTermOccurences {
  constructor(
    public indexTypeName: string,
    // Doc ID => [term, position in doc]
    public docTermOccurrence: Map<number, Array<[bigint, number]>>,
  ) {}
}

/// For an input term doc index, return a sorted list of terms, most popular first.
function generateTermsPopularity(termDocIndex: TermDocOccurrences) {
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

function generateTermDocIndex(
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

      if (term == 240677602n) {
        console.log("TERM", term, "POS", position, "ENTRY", entry);
      }
    }
  }

  console.log("Finished generate term doc occurrences");

  return result;
}

// Combine the docId (as 24 bit unsigned number) and position (as 12 bit unsigned number) into one 32 bit word.
// docId is stored in the low bits, position is stored in the high bits.
function tagEntries(input: Array<[number, number]>): Array<number> {
  // TODO check bounds
  const result = new Array<number>();
  for (const [docId, position] of input) {
    const docIdPart = docId & 0x00FFFFFF;
    const positionPart = (position & 0xFFF) << 24;
    const tagged = docIdPart | positionPart;

    if (tagged) {
      result.push(tagged);
    }
  }
  return result;
}

// Write out pages and manifest for index.
export async function write(
  dbPath: string,
  inputDocOccurrences: DocTermOccurences,
) {
  // Page size. This is measured in entries.
  // This isn't a hard limit, and can be exceeded if a single term doesn't fit.
  // Otherwise, try to pack these as full as possible.
  // This is an abirtrary choice, to be tuned based on characteristics.
  // Stop words play a role in making sure that indiscriminate words (like 'the') are removed.
  const pageSize = 65536 * 2;

  const pagesDir = path.join(
    dbPath,
    "index",
    inputDocOccurrences.indexTypeName,
  );
  Deno.mkdirSync(pagesDir, { recursive: true });

  console.log(
    "Write index of type",
    inputDocOccurrences.indexTypeName,
    "to",
    pagesDir,
  );
  console.log("Generate term doc index...");
  const termDocIndex = generateTermDocIndex(inputDocOccurrences);
  console.log("Generate stats...");

  const longestEntryLength = getLongestEntryLength(termDocIndex);
  const totalEntryLength = getTotalEntriesLength(termDocIndex);

  console.log("Longest entry: ", longestEntryLength);
  console.log("Number of terms: ", termDocIndex.size);
  console.log("Total entry length: ", totalEntryLength);

  console.log("Generate popularity...");

  // Get list of terms in order of popularity.
  const termsPopularity = generateTermsPopularity(termDocIndex);

  // Keep track of terms that were already written to an index.
  // This enables probing for page-packing.
  const alreadyProbedIndexes = new Set<number>();

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
  let pageEntryCount = 0;
  console.log("Start page writing...");

  // Descend the term popularity list and pack the page with the most popular available.
  for (let i = 0; i < termsPopularity.length; i++) {
    // Starting at this term i, probe for entries that can fill the page.
    for (let j = i; j < termsPopularity.length; j++) {
      // Skip this one if it was already used in a prior probe.
      if (alreadyProbedIndexes.has(j)) {
        continue;
      }

      // Get this term.
      const [term, _] = termsPopularity[j];

      const termEntries = termDocIndex.get(term);

      if (term == 240677602n) {
        console.log("TERM", term, "ENTRIES, entries");
      }
      if (!termEntries) continue;

      // Compress the doc occurrence into tagged entries.
      const taggedEntries = tagEntries(termEntries);

      // Bytes required to store this term's worth of data.
      const termRequired = termEntries.length * 4;

      // Handle if this is too big to fit on a page, even by itself.
      // Not handling this case would result in cascading down pages,
      // and not being able to fit in any of them!
      if (termRequired >= pageSize) {
        const oversizedPageBuffer = new Uint32Array(termEntries.length);
        // For file writing.
        const oversizedPageBufferUint8View = new Uint8Array(
          oversizedPageBuffer.buffer,
        );

        manifest.push([term, pageId, pageUsed, taggedEntries.length]);

        for (let z = 0; z < taggedEntries.length; z++) {
          oversizedPageBuffer[pageUsed + z] = taggedEntries[z];
        }

        const pagePath = path.join(pagesDir, "page-" + pageId);
        console.error(
          "Term ",
          term,
          " requires ",
          termRequired,
          "bytes. Save oversized index page to ",
          pagePath,
        );

        await Deno.writeFile(pagePath, oversizedPageBufferUint8View);

        pageId += 1;
        pageBuffer.fill(0);
        pageUsed = 0;
        pageEntryCount = 0;

        continue;
      }

      // Otherwise see if it will fit on this page.
      const willFit = (pageUsed * 4 + termRequired) < pageSize;

      if (willFit) {
        manifest.push([term, pageId, pageUsed, taggedEntries.length]);
        for (let z = 0; z < taggedEntries.length; z++) {
          pageBuffer[pageUsed + z] = taggedEntries[z];
        }
        pageUsed += taggedEntries.length;
        pageEntryCount += 1;

        // If this was done as part of a probe (i.e. j > i) then store to avoid re-using.
        // No need to store if i == j, as 'i' is monotonic.
        if (j > i) {
          alreadyProbedIndexes.add(j);
        }
      }
    }

    // If we didn't put anything on a page it's because everything has been probed.
    // Time to stop scanning.
    if (pageUsed == 0) {
      console.log("Last entry", i);
      break;
    } else {
      // End of probing to fit data in this page. This page is as full as it's going to get.
      const fill = (pageUsed * 4) / pageSize;
      const pagePath = path.join(pagesDir, "page-" + pageId);
      console.log(
        "Save index page to ",
        pagePath,
        "with",
        pageEntryCount,
        "entries, fill factor",
        fill,
      );

      await Deno.writeFile(pagePath, uint8view);

      pageId += 1;
      pageBuffer.fill(0);
      pageUsed = 0;
      pageEntryCount = 0;
    }
  }

  // Write manifest chunked into content-addressable files.
  // Because the distribution of terms is very uneven we can't slice into equal ranges.
  // Instead slice by file size.

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

  const manifestChunkSizeBytes = 64000;

  // Structure of manifest:
  // term : u64
  // page: u32
  // offset: u32
  // length: u32
  // padding: u32
  // Stride of 24 bytes. Aligned to 64 bits.
  const manifestBuf = new ArrayBuffer(manifestChunkSizeBytes);
  const manifestView = new DataView(manifestBuf);

  // Keep track of which term ranges fit on which chunk.
  const manifestManifest = new Array<bigint>();

  let chunkI = 0;
  let offsetInChunk = 0;
  let firstTermInChunk: bigint | null = null;
  for (let i = 0; i < manifest.length; i++) {
    const [term, page, offsetInPage, length] = manifest[i];

    // Keep track of the first term in this chunk to put in the manifest manifest.
    firstTermInChunk = firstTermInChunk || term;

    manifestView.setBigUint64(offsetInChunk, term, true);
    offsetInChunk += 8;
    manifestView.setInt32(offsetInChunk, page, true);
    offsetInChunk += 4;
    manifestView.setInt32(offsetInChunk, offsetInPage, true);
    offsetInChunk += 4;
    manifestView.setInt32(offsetInChunk, length, true);
    offsetInChunk += 4;
    // padding
    offsetInChunk += 4;

    // Flush chunk if the next one will exceed the file size.
    // Or last iteration.
    if (
      (offsetInChunk + 8 + 4 + 4 + 4 + 4 >= manifestChunkSizeBytes) ||
      i == manifest.length - 1
    ) {
      // View of only those used slots.
      const manifest8View = new Uint8Array(manifestBuf.slice(0, offsetInChunk));

      const manifestPath = path.join(pagesDir, "manifest-" + chunkI);
      await Deno.writeFile(manifestPath, manifest8View);
      console.log(
        "Written chunk ",
        chunkI,
        "starting term",
        firstTermInChunk,
        "with",
        offsetInChunk,
        "bytes to",
        manifestPath,
      );

      // Write first term, last term, chunk id.
      manifestManifest.push(firstTermInChunk);
      manifestManifest.push(term);
      manifestManifest.push(BigInt(chunkI));

      firstTermInChunk = null;
      chunkI += 1;
      offsetInChunk = 0;
    }
  }

  // Now write the manifest manifest, which maps term ranges to manifest chunk files.
  const manifestManifest8View = new Uint8Array(
    BigUint64Array.from(manifestManifest).buffer,
  );

  const manifestPath = path.join(pagesDir, "manifest-manifest");
  await Deno.writeFile(manifestPath, manifestManifest8View);

  console.log("Done");
}
