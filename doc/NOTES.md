# 2026-01-01

Hypothesis: text index is a size and shape that allows the searching work to be done in the front-end without prohibitive network traffic or memory use.

Implemented: Build text index from titles. Minimal stemming or normalisation. Term representation is non-lossy.

Corpus: 190,997 tunes

Text index:
 - Longest entry:  41085
 - Total number of terms:  146776
 - Total entry length:  3053303

15 pages that exceed the size. Top 5:

1. 109n 164340  bytes
2. 115n 163748  bytes
3. 98n  162684  bytes
4. 99n  127532  bytes
5. 104n  122596  bytes

Index manifest contains term, page, offset, length, which is 24 bytes per term. Size of text manifest is 3.4 MB which is quite big. Borderline OK, if it's cached.

Might need to reduce the size by splitting up, or by moving length into the index pages, which would save 8 bytes. Reduction of to 2.3 MB.

Next step is to look at the stats for melody index.
