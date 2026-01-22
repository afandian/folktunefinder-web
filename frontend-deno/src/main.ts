import { PagedIndexReaderHttpDriver } from "../../backend/src/httpStorageDriver.ts";
import { IndexSearchQuery, SearchService } from "../../backend/src/search.ts";
import { extractTextTerms } from "../../backend/src/textAnalysis.ts";
import { extractMelodyTerms } from "../../backend/src/melodyAnalysis.ts";
import { TuneDoc } from "../../shared/src/index.ts";
import { pathForId } from "../../backend/src/fileTuneDocDb.ts";

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
  const urlBase = "http://127.0.0.1:8000";

  const driver = new PagedIndexReaderHttpDriver(urlBase);

  const search = new SearchService(
    driver,
    ["melody", "title", "melodyIncipit"],
    32768,
  );

  const queries = new Array<IndexSearchQuery>();
  let results = null;

  if (text) {
    console.log("Search text", text);
    const terms = extractTextTerms([text]);
    queries.push(new IndexSearchQuery("title", terms));
  }

  if (melody) {
    const pitches = melody.split(",").map((x) => parseInt(x) || 0);
    console.log("Search melody", pitches);

    const terms = extractMelodyTerms(pitches);
    queries.push(new IndexSearchQuery("melodyIncipit", terms));
  }

  results = await search.search(queries);

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
    const melody = (<HTMLInputElement> document.getElementById("melody"))
      ?.value;

    navigate(
      new Map([["text", text], ["melody", melody], ["page", "1"]]),
      false,
    );
  };
}
