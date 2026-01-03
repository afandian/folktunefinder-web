export function extractMelodyTerms(melody: Array<number>) {
  const terms = new Array<bigint>();
  const termLength = 5;

  for (let i = 0; i < melody.length - termLength; i++) {
    let term = BigInt(0);
    for (let j = 0; j < termLength; j++) {
      const interval = melody[i + j + 1] - melody[i + j];
      // Avoid overflow. But don't try to clamp.
      if (interval >= -64 && interval < 64) {
        term |= BigInt(interval + 64) << BigInt(j * 8);
      }
    }

    terms.push(term);
  }

  return terms;
}
