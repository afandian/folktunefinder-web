import * as path from "jsr:@std/path";
import {
  getDocsPath,
  iterateDocCollection,
  saveDoc,
  tidyDocCollection,
} from "../fileTuneDocDb.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";

function getConfig() {
  const args = parseArgs(Deno.args);
  if (!args.dbPath) {
    console.error("Supply --dbPath arg");
    Deno.exit(2);
  }

  return args;
}

async function run() {
  const config = getConfig();

  const docsPath = getDocsPath(config.dbPath);

  console.log("Tidy...");
  await tidyDocCollection(docsPath);

  console.log("Load...");

  let count = 0;
  for await (const [path, tuneDoc] of iterateDocCollection(docsPath)) {
    await saveDoc(path, tuneDoc);

    count += 1;
    if (count % 1000 == 0) {
      console.log("Done", count, "...");
    }
  }

  console.log("Done!");
}

run();
