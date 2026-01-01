export class MusicInfo {
  constructor(
    public melodyPitches: Array<number>,
    // List of [Feature type, Feature value, true or false]
    public features: Array<[string, string, boolean]>,
  ) {}

  // deno-lint-ignore-file no-explicit-any
  public static fromJsonObject(input: any) {
    return new MusicInfo(input["melodyPitches"], input["features"]);
  }

  public toJsonObject(): object {
    return { "melodyPitches": this.melodyPitches, "features": this.features };
  }
}

export class TextInfo {
  constructor(
    public titles: Array<string>,
    public misc: Array<string>,
    // Array of ABC text field type, value.
    public textFields: Array<[string, string]>,
  ) {}
  // TODO: Other found text

  public static fromJsonObject(input: any) {
    return new TextInfo(input.titles, input.misc, input.textFields);
  }

  public toJsonObject(): object {
    return {
      "titles": this.titles,
      "misc": this.misc,
      "textFields": this.textFields,
    };
  }
}

export class AnalyzedText {
  constructor(
    // Stemmed normalized text from title.
    public titleText: Array<string>,
    // Stemmed normalised text from other places.
    public otherText: Array<string>,
  ) {}

  public static fromJsonObject(input: any) {
    return new AnalyzedText(input.titleText, input.otherText);
  }

  public toJsonObject(): object {
    return { "titleText": this.titleText, "otherText": this.otherText };
  }
}

// export class DerivedInfo {
//   melodyIntervals: Array<number>;
//   pitchFrequencies: Map<number, number>;
//   intervalFrequencies: Map<number, number>;

//   constructor(
//     melodyIntervals: Array<number>,
//     pitchFreqencies: Map<number, number>,
//     intervalFrequencies: Map<number, number>,
//   ) {
//     this.melodyIntervals = melodyIntervals;
//     this.pitchFrequencies = pitchFreqencies;
//     this.intervalFrequencies = intervalFrequencies;
//   }

//   public toJsonObject(): object {
//     return {
//       "melodyIntervals": this.melodyIntervals,
//       "pitchFrequencies": Object.fromEntries(this.pitchFrequencies),
//       "intervalFrequencies": Object.fromEntries(this.intervalFrequencies),
//     };
//   }
// }

export class TuneDoc {
  constructor(
    // Supplied ID.
    public id: number,
    // List of link type (ABC, Webpage) and URL.
    public links: Array<[string, string]>,
    // Supplied ABC tune.
    public abc: string,
  ) {}

  // Text info derived from the ABC tune.
  derivedText: TextInfo | null = null;

  // Music info derived from the ABC tune.
  derivedMusic: MusicInfo | null = null;

  // Analysis from drived text info.
  analyzedText: AnalyzedText | null = null;

  // Analysis form derived music info.
  // analyzedMusic: AnalyzedMusic | null = null;

  public static fromJsonObject(input: any) {
    // todo dreived
    // const music = MusicInfo.fromJsonObject(input.music);
    const tuneDoc = new TuneDoc(input.id, input.links, input.abc);

    if (input["derivedText"]) {
      tuneDoc.derivedText = TextInfo.fromJsonObject(input.derivedText);
    }

    if (input.derivedMusic) {
      tuneDoc.derivedMusic = MusicInfo.fromJsonObject(input.derivedMusic);
    }

    if (input.analyzedText) {
      tuneDoc.analyzedText = AnalyzedText.fromJsonObject(input.analyzedText);
    }

    return tuneDoc;
  }

  public toJsonObject(): object {
    return {
      "id": this.id,
      "links": this.links,
      "abc": this.abc,
      "derivedText": this.derivedText?.toJsonObject(),
      "derivedMusic": this.derivedMusic?.toJsonObject(),
      "analyzedText": this.analyzedText?.toJsonObject(),
    };
  }
}

function generateMelodyIntervals(melodyPitches: Array<number>) {
  console.log("MP", melodyPitches, melodyPitches.length);
  const result = new Array<number>(melodyPitches.length - 1);
  if (melodyPitches.length < 2) {
    return result;
  }

  let i = 0;
  for (i = 0; i < melodyPitches.length - 1; i++) {
    result[i] = melodyPitches[i + 1] - melodyPitches[i];
  }

  return result;
}

function generatePitchFrequencies(melodyPitches: Array<number>) {
  const result = new Map<number, number>();

  melodyPitches.forEach((pitch) => {
    result.set(pitch, (result.get(pitch) || 0) + 1);
  });

  return result;
}

function generateIntervalFrequencies(melodyIntervals: Array<number>) {
  const result = new Map<number, number>();
  melodyIntervals.forEach((interval) => {
    result.set(interval, (result.get(interval) || 0) + 1);
  });

  return result;
}

// Just for proof-of-concept linking from front-end.
export const greeting = "hello";
