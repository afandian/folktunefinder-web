import { iterateDocCollection } from "../fileTuneDocDb.ts";
import { DocTermOccurences } from "../indexWriter.ts";
import { extractMelodyTerms } from "../melodyIndex.ts";

export async function generateMelodyIndex(docsPath: string) {
  const docOccurrences = new Map<number, Array<[bigint, number]>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedMusic?.melodyPitches) {
      docOccurrences.set(
        tuneDoc.id,
        extractMelodyTerms(tuneDoc.derivedMusic?.melodyPitches),
      );
    }
    count += 1;
    if (count % 10000 == 0) {
      console.log("Read", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new DocTermOccurences("melodyIndex", docOccurrences);
}

export async function generateMelodyIncipitIndex(docsPath: string) {
  const docOccurrences = new Map<number, Array<[bigint, number]>>();

  let count = 0;
  for await (const [_, tuneDoc] of iterateDocCollection(docsPath)) {
    if (tuneDoc.derivedMusic?.melodyPitches) {
      const incipitPitches = tuneDoc.derivedMusic?.melodyPitches.slice(0, 12);

      docOccurrences.set(
        tuneDoc.id,
        extractMelodyTerms(incipitPitches),
      );
    }

    count += 1;
    if (count % 10000 == 0) {
      console.log("Read", count, "...");
    }
  }

  console.log("Read total", count, "docs.");

  return new DocTermOccurences("melodyIncipitIndex", docOccurrences);
}
