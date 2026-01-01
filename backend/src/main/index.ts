import * as path from "jsr:@std/path";
import { getDocsPath, iterateDocCollection } from "../fileTuneDocDb.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";

function getConfig() {
  const args = parseArgs(Deno.args);
  if (!args.dbPath) {
    console.error("Supply --dbPath arg");
    Deno.exit(2);
  }

  return args;
}

const stopWords = new Set(["the"]);

// Tokenize a string, with some normalization and stemming.
function tokenizeWords(words: string) {
  // TODO split words better.
  // TODO normalise diacritics
  const splitWords = words.toLowerCase().split(/ /);

  const results = new Array<string>();

  for (const word of splitWords) {
    let result = word;
    result = result.trim().toLowerCase();
    // TODO stemming
    if (result.endsWith("s")) {
      result = result.substring(0, result.length);
    }

    // Remove short stop words. Unless they are numbers, which can be kept as they can be useful in searching.
    if (result.length < 3 && result.match(/[a-z]+/)) {
      continue;
    }

    if (stopWords.has(word)) {
      continue;
    }

    // O(n) but most titles are 2 long.
    // TODO verify lengths of titles.
    if (!results.includes(result)) {
      results.push(result);
    }
  }

  return results;
}

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

class TermDocIndex {
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
async function outputPages(
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

function extractTextTerms(inputs: Array<string>) {
  const terms = new Array<bigint>();

  // There be multiple due to multiple titles, which can be repetitive with different spellings.
  // Combine these prior to tokenizing, so we can deduplicate across them all.
  const combined = inputs.join(" ");

  const tokens = tokenizeWords(combined);

  for (const token of tokens) {
    // Take the first 9 chars.
    const length = Math.min(9, token.length);
    let term = BigInt(0);

    for (let i = 0; i < length; i++) {
      // Take only the lower 7 bits. These are mostly ASCII, so we don't need the top bit.
      // This lets us squeeze another character into 64 bits.
      term |= BigInt(token.charCodeAt(i) & 0x7F) << BigInt(i * 7);

      // console.log(term);
      terms.push(term);
    }
  }

  return terms;
}

async function generateTextIndex(docsPath: string) {
  const textIndex = new Map<bigint, Array<number>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedText?.titles) {
      const textTerms = extractTextTerms(tuneDoc.derivedText?.titles);
      for (const term of textTerms) {
        const forTerm = textIndex.get(term);
        if (forTerm) {
          forTerm.push(tuneDoc.id);
        } else {
          textIndex.set(term, [tuneDoc.id]);
        }
      }
    }

    count += 1;
    if (count % 1000 == 0) {
      console.log("Done", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new TermDocIndex("titleText", textIndex);
}

async function run() {
  const config = getConfig();

  const docsPath = getDocsPath(config.dbPath);

  console.log("Load and generate text index...");

  const textIndex = await generateTextIndex(docsPath);

  console.log("Output text index pages...");

  outputPages(config.dbPath, textIndex);

  console.log("Done!");
}

run();
