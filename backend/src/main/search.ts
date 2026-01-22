import { PagedIndexReaderFileDriver } from "../diskStorageDriver.ts";
import { parseArgs } from "jsr:@std/cli/parse-args";
import { extractTextTerms } from "../textAnalysis.ts";
import { extractMelodyTerms } from "../melodyAnalysis.ts";
import { IndexSearchQuery, SearchService } from "../search.ts";

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

  const searchDriver = new PagedIndexReaderFileDriver(config.dbPath);

  const search = new SearchService(
    searchDriver,
    //["melody", "title", "melodyIncipit"],
    ["title"],
    32768,
  );

  const searchQueries = Array<IndexSearchQuery>();

  if (config.titleSearch) {
    const text = config.titleSearch;

    const terms = extractTextTerms([text]);
    searchQueries.push(new IndexSearchQuery("title", terms));
  }

  if (config.melodySearch) {
    const text = config.melodySearch;
    if (typeof text == "string") {
      const numbers = text.split(",").map((x) => {
        return parseInt(x);
      });

      const terms = extractMelodyTerms(numbers);
      searchQueries.push(new IndexSearchQuery("melodyIncipit", terms));
    }
  }

  const result = await search.search(searchQueries);
  console.log(searchQueries);
  console.log(result);
}

run();
