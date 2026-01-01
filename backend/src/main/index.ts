import { getDocsPath } from "../fileTuneDocDb.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";
import { generateTextIndex } from "./textIndex.ts";
import { outputPages } from "../index.ts";

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

  console.log("Load and generate text index...");

  const textIndex = await generateTextIndex(docsPath);

  console.log("Output text index pages...");

  outputPages(config.dbPath, textIndex);

  console.log("Done!");
}

run();
