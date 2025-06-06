/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.queries.spans;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * A basic {@link Scorer} over {@link Spans}.
 *
 * @lucene.experimental
 */
public class SpanScorer extends Scorer {

  protected final Spans spans;
  protected final SimScorer scorer;
  protected final NumericDocValues norms;

  /** accumulated sloppy freq (computed in setFreqCurrentDoc) */
  private float freq;

  private int lastScoredDoc = -1; // last doc we called setFreqCurrentDoc() for

  /** Sole constructor. */
  public SpanScorer(Spans spans, SimScorer scorer, NumericDocValues norms) {
    this.spans = Objects.requireNonNull(spans);
    this.scorer = scorer;
    this.norms = norms;
  }

  /** return the Spans for this Scorer * */
  public Spans getSpans() {
    return spans;
  }

  @Override
  public int docID() {
    return spans.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return spans;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return spans.asTwoPhaseIterator();
  }

  /**
   * Score the current doc. The default implementation scores the doc with the similarity using the
   * slop-adjusted {@link #freq}.
   */
  protected float scoreCurrentDoc() throws IOException {
    assert scorer != null : getClass() + " has a null docScorer!";
    long norm = 1L;
    if (norms != null && norms.advanceExact(docID())) {
      norm = norms.longValue();
    }
    return scorer.score(freq, norm);
  }

  /**
   * Sets {@link #freq} for the current document.
   *
   * <p>This will be called at most once per document.
   */
  protected final void setFreqCurrentDoc() throws IOException {
    freq = 0.0f;

    spans.doStartCurrentDoc();

    assert spans.startPosition() == -1 : "incorrect initial start position, " + spans;
    assert spans.endPosition() == -1 : "incorrect initial end position, " + spans;
    int prevStartPos = -1;
    int prevEndPos = -1;

    int startPos = spans.nextStartPosition();
    assert startPos != Spans.NO_MORE_POSITIONS : "initial startPos NO_MORE_POSITIONS, " + spans;
    do {
      assert startPos >= prevStartPos;
      int endPos = spans.endPosition();
      assert endPos != Spans.NO_MORE_POSITIONS;
      // This assertion can fail for Or spans on the same term:
      // assert (startPos != prevStartPos) || (endPos > prevEndPos) : "non increased
      // endPos="+endPos;
      assert (startPos != prevStartPos) || (endPos >= prevEndPos) : "decreased endPos=" + endPos;
      if (scorer == null) { // scores not required, break out here
        freq = 1;
        return;
      }
      freq += (1.0 / (1.0 + spans.width()));
      spans.doCurrentSpans();
      prevStartPos = startPos;
      prevEndPos = endPos;
      startPos = spans.nextStartPosition();
    } while (startPos != Spans.NO_MORE_POSITIONS);

    assert spans.startPosition() == Spans.NO_MORE_POSITIONS
        : "incorrect final start position, " + spans;
    assert spans.endPosition() == Spans.NO_MORE_POSITIONS
        : "incorrect final end position, " + spans;
  }

  /** Ensure setFreqCurrentDoc is called, if not already called for the current doc. */
  private void ensureFreq() throws IOException {
    int currentDoc = docID();
    if (lastScoredDoc != currentDoc) {
      setFreqCurrentDoc();
      lastScoredDoc = currentDoc;
    }
  }

  @Override
  public final float score() throws IOException {
    ensureFreq();
    return scoreCurrentDoc();
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return Float.POSITIVE_INFINITY;
  }

  /**
   * Returns the intermediate "sloppy freq" adjusted for edit distance
   *
   * @lucene.internal
   */
  final float sloppyFreq() throws IOException {
    ensureFreq();
    return freq;
  }
}
