import { iterateDocCollection } from "./fileTuneDocDb.ts";
import { DocTermOccurences, TermDocIndex } from "./indexWriter.ts";

const stopWords = new Set(["the", "is", "it"]);

const stems = ["s", "ed", "ing", "ly", "es"];

// Tokenize a string, with some normalization and stemming.
function tokenizeWords(words: string) {
  // TODO split words better.
  // TODO normalise diacritics
  const splitWords = words.toLowerCase().split(/ /);

  const results = new Array<string>();

  for (const word of splitWords) {
    if (!word) continue;

    let result = word;
    result = result.trim().toLowerCase();

    // Remove short stop words. Unless they are numbers, which can be kept as they can be useful in searching.
    // Do this before stemming in case stemming removes words that would have been matched (e.g. "ted" -> "t")
    if (result.length < 3 && result.match(/[a-z]+/)) {
      continue;
    }

    for (const stem of stems) {
      if (result.endsWith(stem)) {
        result = result.substring(0, result.length - stem.length);
      }
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

// Analyzer to produce set of terms from input text.
// Used for indexing and search.
export function extractTextTerms(inputs: Array<string>) {
  const terms = new Array<[bigint, number]>();

  // There be multiple due to multiple titles, which can be repetitive with different spellings.
  // Combine these prior to tokenizing, so we can deduplicate across them all.
  const combined = inputs.join(" ");

  const tokens = tokenizeWords(combined);

  let i = 0;
  for (const token of tokens) {
    // Take the first 9 chars.
    const length = Math.min(9, token.length);
    let term = BigInt(0);

    for (let i = 0; i < length; i++) {
      // Take only the lower 7 bits. These are mostly ASCII, so we don't need the top bit.
      // This lets us squeeze another character into 64 bits.
      term |= BigInt(token.charCodeAt(i) & 0x7F) << BigInt(i * 7);
    }
    terms.push([term, i]);
    i += 1;
  }

  return terms;
}

export async function generateTextIndex(docsPath: string) {
  const docOccurrences = new Map<number, Array<[bigint, number]>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedText?.titles) {
      const textTerms = extractTextTerms(tuneDoc.derivedText?.titles);
      docOccurrences.set(tuneDoc.id, textTerms);
    }

    count += 1;
    if (count % 1000 == 0) {
      console.log("Done", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new DocTermOccurences("titleText", docOccurrences);
}
