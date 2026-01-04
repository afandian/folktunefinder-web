import { Resolver } from "../../backend/src/indexReader.ts";
import { SearchService } from "../../backend/src/search.ts";
import { extractTextTerms } from "../../backend/src/textIndex.ts";

class HttpResolver implements Resolver {
  constructor(public dbPath: string) {}

  // Number of network requests.
  private requests = 0;

  // Number of request bytes.
  private requestBytes = 0;

  async loadManifestManifestForType(indexType: string) {
    const filePath = this.dbPath +
      "/index/" +
      indexType +
      "/manifest-manifest";

    const responseBuffer = await (await fetch(filePath)).arrayBuffer();

    this.requests += 1;
    this.requestBytes += responseBuffer.byteLength;

    // Take a copy of the buffer.
    return new BigUint64Array(responseBuffer);
  }

  async getManifestChunk(indexType: string, chunkId: number) {
    const filePath = this.dbPath +
      "/index/" +
      indexType +
      "/manifest-" + chunkId;

    const responseBuffer = await (await fetch(filePath)).arrayBuffer();

    this.requests += 1;
    this.requestBytes += responseBuffer.byteLength;

    // Take a copy of the buffer.
    return new BigUint64Array(responseBuffer);
  }

  async getPageId(indexType: string, pageId: number) {
    const filePath = this.dbPath +
      "/index/" +
      indexType +
      "/page-" + pageId;

    const responseBuffer = await (await fetch(filePath)).arrayBuffer();

    this.requests += 1;
    this.requestBytes += responseBuffer.byteLength;

    return new Uint32Array(responseBuffer);
  }

  getTotalRequests(): number {
    return this.requests;
  }

  getTotalRequestBytes(): number {
    return this.requestBytes;
  }
}

export async function searchMain(text: string | null) {
  const resultDiv = document.getElementById("result");
  if (!resultDiv) {
    return;
  }

  const resolver = new HttpResolver("http://localhost:8000");

  const search = new SearchService(resolver);
  search.initType("titleText");
  search.initType("melodyIndex");
  search.initType("melodyIncipitIndex");

  if (text) {
    console.log("Search text", text);
    const terms = extractTextTerms([text]);
    const results = await search.search("titleText", terms);
    if (results) {
      const section = document.createElement("section");
      section.textContent = "Loading...";
      resultDiv.appendChild(section);

      const ul = document.createElement("ul");
      section.appendChild(ul);

      for (const [docId, _score] of results.result) {
        const li = document.createElement("li");
        const a = document.createElement("a");
        a.textContent = "Tune " + docId.toString();
        a.setAttribute("href", "https://folktunefinder.com/tunes/" + docId);

        li.appendChild(a);
        ul.appendChild(li);
      }
    } else {
      const fragment = document.createDocumentFragment();

      const p = fragment
        .appendChild(document.createElement("section"))
        .appendChild(document.createElement("p"));
      p.textContent = "No results found";
      resultDiv.appendChild(fragment);
    }
  }
}

const submitButton = document.getElementById("submit");
if (submitButton) {
  submitButton.onclick = (event) => {
    event.preventDefault();
    const text = (<HTMLInputElement> document.getElementById("textSearch"))
      ?.value;
    if (text) {
      searchMain(text);
    }
  };
}
