# Folk Tune Finder Browser edition

Experimental. Written in TypeScript. Mostly a proof-of-concept to see if FTF can run without a back-end API, using only static files. Also an exercise to learn TypeScript. No AI!  

Back-end runs in Deno to produce static database files.

Front-end runs in the browser, consuming the static database files over HTTP. Shares some code with back-end.

## Operation

Database is a structure of files. See 'data structures'.

Load from Folk Tune Finder digest dump

Take a database dump from the Clojure folktunefinder. Import into the database.

```
deno --allow-read  --allow-write src/main/loadFromDigests.ts --dbPath=/home/joe/sc/ftfdb --digestPath=../db/digests2
```

Tidy tune database files

Put TuneDoc files in deterministic places. `loadFromDigests` will already do this, but running this ensures nothing gets confused.

```
deno --allow-read  --allow-write src/main/tidy.ts --dbPath=/home/joe/sc/ftfdb
```

Build indexes

Read and analyze all TuneDocs and produce index files.

```
deno --allow-read  --allow-write src/main/index.ts --dbPath=/home/joe/sc/ftfdb
```

Build frontend
```
(cd frontend && npm run build)
```

## Data structures

All index terms are expressed as uint64. Text tokens are squeezed into these terms. So are melody interval terms. 

Tune IDs are uint32s.

### Tunes

Files stored at `<TUNE_DB>/docs/`. JSON files containing ABC, and other derived information. The filename is an integer which denotes the ID of the tune. This structure kept correct by the `tidy.ts` command.

### Index

An index is stored at `<TUNE_DB>/indexes/<INDEX_TYPE>/`. It consists of an inverted index, which maps terms to occurrence in documents. There may be multiple types of indexes. Idex types currently include only `title`.

The index is built fresh each time, so there's no need for expandable tree data types. Everything is stored in fixed-size structures.

Occurrences are stored in pages. These are of a fixed size, with a constant found in the code. Currently 64 KB. The index pages are filled in order of term popularity, with more popular terms occurring in earlier pages. Pages are packed for maximum fill-factor, and experimentally, most of them achieve over 0.99 fill factor. 

There are some terms (fewer than 10) which have entry lists exceeding the size of the page. These pages are allowed to exist as exceptions. Otherwise we'd have to increase page size to an inconvenient size, which isn't worth it for a handful of exceptions. There's also no point handling multi-page indexes given that we'd have to retrieve the same data in any case.

Manifest file is called `manifest`. It is a mapping of 64-bit term to index page and offset. The file is sorted for quick finding of terms.

Index pages are called `page-<NUMBER>` and contain entries of tune doc occurrences as u32s. It is indexed into by the manifest.

The manifest is sorted by term.
