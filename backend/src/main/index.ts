import { getDocsPath } from "../fileTuneDocDb.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";
import { extractTextTerms, generateTextIndex } from "../textAnalysis.ts";
import { generateTermDocIndex, PagedIndexWriter } from "../index.ts";
import {
  generateMelodyIncipitDocTermOccurrences,
  generateMelodyIndex,
} from "./musicIndex.ts";
import v8 from "node:v8";
import { PagedIndexWriterFileDriver } from "../diskStorageDriver.ts";
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

  const driver = new PagedIndexWriterFileDriver(config.dbPath);

  if (true) {
    const demoIndex = new Map<bigint, Array<[number, number]>>();
    for (let t = 0n; t < 2000n; t += 1n) {
      const o: Array<[number, number]> = [];
      for (let d = 0; d < t; d += 1) {
        o.push([Number(d), 1]);
      }
      demoIndex.set(BigInt(t), o);
    }
    const demoWriter = new PagedIndexWriter(32768, driver, "demo");
    await demoWriter.write(demoIndex);
  }

  if (true) {
    console.log("Load and generate text index...");
    const textDocTermOccurrences = await generateTextIndex(docsPath);
    console.log("Generate inverted index...");
    const textIndex = generateTermDocIndex(
      textDocTermOccurrences,
    );
    console.log("DANCE", textIndex.get(27321413860n));
    console.log("Output textIndex pages...");
    const textWriter = new PagedIndexWriter(32768, driver, "title");
    await textWriter.write(textIndex);
  }

  if (true) {
    console.log("Load and generate melody incipit index...");
    const melodyIncipitDocTermOccurrences =
      await generateMelodyIncipitDocTermOccurrences(docsPath);
    const melodyIncipitIndex = generateTermDocIndex(
      melodyIncipitDocTermOccurrences,
    );
    console.log("Output melodyIndex pages...");
    const melodyIncipitWriter = new PagedIndexWriter(
      32768,
      driver,
      "melodyIncipit",
    );
    await melodyIncipitWriter.write(melodyIncipitIndex);
  }

  if (true) {
    console.log("Load and generate melody index...");
    const melodyDocTermOccurrences = await generateMelodyIndex(docsPath);
    const melodyIndex = generateTermDocIndex(
      melodyDocTermOccurrences,
    );

    console.log("Output melodyIndex pages...");
    const melodyWriter = new PagedIndexWriter(32768, driver, "melody");
    await melodyWriter.write(melodyIndex);
  }
  console.log("Done!");
}

await run();
