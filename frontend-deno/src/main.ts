import { Resolver } from "../../backend/src/indexReader.ts";
import { SearchService } from "../../backend/src/search.ts";
import { extractTextTerms } from "../../backend/src/textIndex.ts";
import { TuneDoc } from "../../shared/src/index.ts";
import { pathForId } from "../../backend/src/fileTuneDocDb.ts";

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

export async function searchMain(
  text: string | null,
  melody: string | null,
  page: number,
) {
  const resultDiv = document.getElementById("result");
  if (!resultDiv) {
    return;
  }

  const pageSize = 20;
  const urlBase = "http://localhost:8000";
  const resolver = new HttpResolver(urlBase);
  const search = new SearchService(resolver);
  search.initType("titleText");
  search.initType("melodyIndex");
  search.initType("melodyIncipitIndex");

  // TODO multiple search types.
  let results = null;
  if (text) {
    console.log("Search text", text);
    const terms = extractTextTerms([text]);
    results = await search.search("titleText", terms);
  }

  if (results) {
    let nextPage = null;
    if (results.result.length > page * pageSize) {
      nextPage = page + 1;
    }
    let prevPage = null;
    if (page > 1) {
      prevPage = page - 1;
    }
    const numPages = Math.ceil(results.result.length / pageSize);

    const section = document.createElement("section");
    section.textContent = "Loading...";
    resultDiv.replaceChildren(section);

    const ul = document.createElement("ul");
    section.appendChild(ul);

    for (
      let i = (page - 1) * pageSize;
      i < Math.min(results.result.length, page * pageSize);
      i++
    ) {
      const [docId, _score] = results.result[i];
      const li = document.createElement("li");
      const a = document.createElement("a");
      a.textContent = "Tune " + docId.toString();
      a.setAttribute("href", "https://folktunefinder.com/tunes/" + docId);

      const path = pathForId("/docs/", docId.toString());
      fetch(urlBase + path).then(async (result) => {
        const doc = TuneDoc.fromJsonObject(await result.json());
        const title = doc.derivedText?.titles?.join(" / ") || "Unknown";
        a.textContent = title;
      });

      li.appendChild(a);
      ul.appendChild(li);
    }

    const nextPrev = document.createElement("div");
    resultDiv.appendChild(nextPrev);

    const summary = document.createElement("p");
    summary.innerText =
      `Found ${results.result.length} results. Page ${page} of ${numPages}`;
    resultDiv.appendChild(summary);

    if (prevPage) {
      const prev = document.createElement("a");
      prev.innerText = "Previous";
      resultDiv.appendChild(prev);
      prev.onclick = () => {
        navigate(new Map([["page", prevPage.toString()]]), false);
      };
    }

    if (nextPage) {
      const next = document.createElement("a");
      next.innerText = "Next";
      resultDiv.appendChild(next);
      next.onclick = () => {
        navigate(new Map([["page", nextPage.toString()]]), false);
      };
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

// Page URL was changed, so re-search.
// Called on first landing, popstate event, and explicitly on navigate()
function navigated() {
  const urlSearchParams = new URLSearchParams(window.location.search);
  const text = urlSearchParams.get("text");
  const melody = urlSearchParams.get("melody");
  const page = parseInt(urlSearchParams.get("page") || "1") || 1;

  searchMain(text, melody, page);
}

// Call first load.
navigated();

addEventListener("popstate", navigated);

function navigate(params: Map<string, string>, replace: bool) {
  const url = new URL(location);
  if (replace) {
    url.search = "";
  }

  for (const [k, v] of params) {
    url.searchParams.set(k, v);
  }
  history.pushState({}, "", url);

  navigated();
}

const submitButton = document.getElementById("submit");
if (submitButton) {
  submitButton.onclick = (event) => {
    event.preventDefault();
    const text = (<HTMLInputElement> document.getElementById("text"))
      ?.value;
    navigate(new Map([["text", text], ["page", "1"]]), false);
  };
}
