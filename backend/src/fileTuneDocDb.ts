import * as path from "jsr:@std/path";

import { TuneDoc } from "../../shared/src/index.ts";

export function getDocsPath(dbPath: string) {
  return path.join(dbPath, "docs");
}

/// Produce predictable path for the doc ID.
/// Create directory structure if needed.
export function pathForId(docsPath: string, docId: string) {
  // Chunks in path.
  const chunkSize = 2;

  const expectedPathParts = Array<string>();

  let i = 0;
  for (i = 0; i < docId.length; i += chunkSize) {
    expectedPathParts.push(docId.substring(i, i + chunkSize));
  }

  const expectedPathDirs = path.join(docsPath, ...expectedPathParts);

  expectedPathParts.push(docId + ".json");

  const expectedPath = path.join(docsPath, ...expectedPathParts);

  Deno.mkdirSync(expectedPathDirs, { recursive: true });

  return expectedPath;
}

// Tidy up the Tune Document directory structure into a predictable shape.
// This is important when there's a database of 200,000 files.
export async function tidyDocCollection(docsPath: string) {
  async function scan(currPath: string) {
    for await (const dirEntry of Deno.readDir(currPath)) {
      const fullPath = path.join(currPath, dirEntry.name);

      if (dirEntry.isDirectory) {
        await scan(fullPath);
      } else if (dirEntry.isFile) {
        const docIdRe = dirEntry.name.match(/(\d+).json/);
        if (docIdRe == null || docIdRe.length != 2) {
          console.error(
            "Unexpeced file format",
            dirEntry.name,
            ", Skipping.",
          );
          return null;
        }

        const docId = docIdRe[1];
        const expectedPath = pathForId(docsPath, docId);

        if (!pathForId) {
          console.error("Couldn't normalize path for", fullPath);
          return null;
        }
        if (expectedPath != fullPath) {
          console.log("Move", fullPath, "to", expectedPath);
          Deno.rename(fullPath, expectedPath);
        }
      }
    }
  }

  await scan(docsPath);
}

export async function* iterateDocCollection(
  docsPath: string,
): AsyncGenerator<[string, TuneDoc], void, void> {
  async function* scan(
    basePath: string,
  ): AsyncGenerator<[string, TuneDoc], void, void> {
    for await (const dirEntry of Deno.readDir(basePath)) {
      const fullPath = path.join(basePath, dirEntry.name);
      if (dirEntry.isDirectory) {
        yield* scan(fullPath);
      } else if (dirEntry.isFile) {
        try {
          const json = await Deno.readTextFile(fullPath);
          const parsed = JSON.parse(json);

          const tuneDbObject: TuneDoc = TuneDoc.fromJsonObject(parsed);

          yield [fullPath, tuneDbObject];
        } catch (exception) {
          console.error("Error reading tune", path, exception);
        }
      }
    }
  }

  yield* scan(docsPath);
}

export async function saveDoc(docsPath: string, tuneDoc: TuneDoc) {
  await Deno.writeTextFile(docsPath, JSON.stringify(tuneDoc.toJsonObject()));
}
