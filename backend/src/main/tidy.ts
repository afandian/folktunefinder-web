import * as path from "jsr:@std/path";
import { generateDerived } from "../../../shared/src/index.ts";
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
    // const derived = generateDerived(tuneDoc.);
    // console.log("DERIVED", derived);
    // tuneDoc.derived = derived;

    await saveDoc(path, tuneDoc);

    count += 1;
    if (count % 1000 == 0) {
      console.log("Done", count, "...");
    }
  }

  //saveDocCollection(docs, config.dbPath);

  // console.log(docs);
  //
  console.log("Done!");
}

run();
