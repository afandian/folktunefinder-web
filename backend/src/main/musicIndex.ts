import { iterateDocCollection } from "../fileTuneDocDb.ts";
import { TermDocIndex } from "../index.ts";

function extractMelodyTerms(melody: Array<number>) {
  const terms = new Array<bigint>();
  const termLength = 5;

  for (let i = 0; i < melody.length - termLength; i++) {
    let term = BigInt(0);
    for (let j = 0; j < termLength; j++) {
      const interval = melody[i + j + 1] - melody[i + j];

      // Avoid overflow. But don't try to clamp.
      if (interval >= -64 && interval < 64) {
        term |= BigInt(interval + 64) << BigInt(j * 8);
      }
    }

    terms.push(term);
  }

  return terms;
}

export async function generateMelodyIndex(docsPath: string) {
  const textIndex = new Map<bigint, Array<number>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedMusic?.melodyPitches) {
      const terms = extractMelodyTerms(tuneDoc.derivedMusic?.melodyPitches);
      for (const term of terms) {
        const forTerm = textIndex.get(term);
        if (forTerm) {
          forTerm.push(tuneDoc.id);
        } else {
          textIndex.set(term, [tuneDoc.id]);
        }
      }
    }

    count += 1;
    if (count % 10000 == 0) {
      console.log("Read", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new TermDocIndex("melodyIndex", textIndex);
}

export async function generateMelodyIncipitIndex(docsPath: string) {
  const textIndex = new Map<bigint, Array<number>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedMusic?.melodyPitches) {
      const pitches = tuneDoc.derivedMusic?.melodyPitches.slice(0, 12);

      const terms = extractMelodyTerms(pitches);
      for (const term of terms) {
        const forTerm = textIndex.get(term);
        if (forTerm) {
          forTerm.push(tuneDoc.id);
        } else {
          textIndex.set(term, [tuneDoc.id]);
        }
      }
    }

    count += 1;
    if (count % 10000 == 0) {
      console.log("Read", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new TermDocIndex("melodyIncipitIndex", textIndex);
}
