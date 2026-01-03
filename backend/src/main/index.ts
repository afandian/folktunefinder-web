import { getDocsPath } from "../fileTuneDocDb.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";
import { generateTextIndex } from "../textIndex.ts";
import { write } from "../indexWriter.ts";
import {
  generateMelodyIncipitIndex,
  generateMelodyIndex,
} from "./musicIndex.ts";

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

  console.log("Load and generate melody incipit index...");

  const melodyIncipitIndex = await generateMelodyIncipitIndex(docsPath);

  console.log("Output melody incipit pages...");

  await write(config.dbPath, melodyIncipitIndex);

  console.log("Load and generate melody index...");

  const melodyIndex = await generateMelodyIndex(docsPath);

  console.log("Output melody index pages...");

  await write(config.dbPath, melodyIndex);

  console.log("Load and generate text index...");

  const textIndex = await generateTextIndex(docsPath);

  console.log("Output text index pages...");

  write(config.dbPath, textIndex);

  console.log("Done!");
}

run();
