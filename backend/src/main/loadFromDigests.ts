import * as path from "jsr:@std/path";

import { parseArgs } from "jsr:@std/cli/parse-args";
import { TextLineStream } from "jsr:@std/streams/text-line-stream";
import { JsonParseStream } from "jsr:@std/json/parse-stream";
import { MusicInfo, TextInfo, TuneDoc } from "../../../shared/src/index.ts";
import { getDocsPath, pathForId, saveDoc } from "../fileTuneDocDb.ts";
function getConfig() {
  const args = parseArgs(Deno.args);
  if (!args.dbPath) {
    console.error("Supply --dbPath arg");
    Deno.exit(2);
  }

  if (!args.digestPath) {
    console.error("Supply --digestPath arg");
    Deno.exit(2);
  }

  return args;
}

async function run() {
  const config = getConfig();
  const docsPath = getDocsPath(config.dbPath);

  const file = await Deno.open(config.digestPath);

  const readable = file.readable
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(new TextLineStream())
    .pipeThrough(new JsonParseStream());

  let count = 0;
  for await (const data of readable) {
    const docId: number = data.id;
    const abc: string = data.abc;

    const geometry: Array<[number, number]> = data.digest.geometry;
    const melodyPitches = geometry.map(([_, y]) => {
      return y;
    });
    const links: Array<[string, string]> = data.digest.links;
    const titles: Array<string> = data.digest.titles;
    const textIndex: Array<string> = data.digest["text-index"];
    const groupFeatures: Array<[string, string, boolean]> =
      data["group-features"];
    const text: Array<[string, string]> = data.digest.text;

    const tuneDoc = new TuneDoc(docId, links, abc);
    tuneDoc.derivedText = new TextInfo(titles, textIndex, text);
    tuneDoc.derivedMusic = new MusicInfo(melodyPitches, groupFeatures);

    const docPath = pathForId(docsPath, docId.toString());

    Deno.mkdirSync(path.dirname(docPath), { recursive: true });

    saveDoc(docPath, tuneDoc);

    count += 1;
    if (count % 1000 == 0) {
      console.log("Written ", count, "...");
    }
  }
}

run();
