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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.NumericUtils;

final class LongDistanceFeatureQuery extends Query {
  // MIN_SKIP_INTERVAL and MAX_SKIP_INTERVAL both should be powers of 2
  private static final int MIN_SKIP_INTERVAL = 32;
  private static final int MAX_SKIP_INTERVAL = 8192;

  private final String field;
  private final long origin;
  private final long pivotDistance;

  LongDistanceFeatureQuery(String field, long origin, long pivotDistance) {
    this.field = Objects.requireNonNull(field);
    this.origin = origin;
    if (pivotDistance <= 0) {
      throw new IllegalArgumentException("pivotDistance must be > 0, got " + pivotDistance);
    }
    this.pivotDistance = pivotDistance;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(LongDistanceFeatureQuery other) {
    return Objects.equals(field, other.field)
        && origin == other.origin
        && pivotDistance == other.pivotDistance;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Long.hashCode(origin);
    h = 31 * h + Long.hashCode(pivotDistance);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + "(field="
        + field
        + ",origin="
        + origin
        + ",pivotDistance="
        + pivotDistance
        + ")";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        SortedNumericDocValues multiDocValues = DocValues.getSortedNumeric(context.reader(), field);
        if (multiDocValues.advanceExact(doc) == false) {
          return Explanation.noMatch(
              "Document " + doc + " doesn't have a value for field " + field);
        }
        long value = selectValue(multiDocValues);
        long distance = Math.max(value, origin) - Math.min(value, origin);
        if (distance < 0) {
          // underflow, treat as MAX_VALUE
          distance = Long.MAX_VALUE;
        }
        float score = (float) (boost * (pivotDistance / (pivotDistance + (double) distance)));
        return Explanation.match(
            score,
            "Distance score, computed as weight * pivotDistance / (pivotDistance + abs(value - origin)) from:",
            Explanation.match(boost, "weight"),
            Explanation.match(pivotDistance, "pivotDistance"),
            Explanation.match(origin, "origin"),
            Explanation.match(value, "current value"));
      }

      private long selectValue(SortedNumericDocValues multiDocValues) throws IOException {
        int count = multiDocValues.docValueCount();

        long next = multiDocValues.nextValue();
        if (count == 1 || next >= origin) {
          return next;
        }
        long previous = next;
        for (int i = 1; i < count; ++i) {
          next = multiDocValues.nextValue();
          if (next >= origin) {
            // Unsigned comparison because of underflows
            if (Long.compareUnsigned(origin - previous, next - origin) < 0) {
              return previous;
            } else {
              return next;
            }
          }
          previous = next;
        }

        assert next < origin;
        return next;
      }

      private NumericDocValues selectValues(SortedNumericDocValues multiDocValues) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(multiDocValues);
        if (singleton != null) {
          return singleton;
        }
        return new NumericDocValues() {

          long value;

          @Override
          public long longValue() throws IOException {
            return value;
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            if (multiDocValues.advanceExact(target)) {
              value = selectValue(multiDocValues);
              return true;
            } else {
              return false;
            }
          }

          @Override
          public int docID() {
            return multiDocValues.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return multiDocValues.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return multiDocValues.advance(target);
          }

          @Override
          public long cost() {
            return multiDocValues.cost();
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        PointValues pointValues = context.reader().getPointValues(field);
        if (pointValues == null) {
          // No data on this segment
          return null;
        }
        final SortedNumericDocValues multiDocValues =
            DocValues.getSortedNumeric(context.reader(), field);
        final NumericDocValues docValues = selectValues(multiDocValues);
        return new ScorerSupplier() {

          @Override
          public Scorer get(long leadCost) throws IOException {
            return new DistanceScorer(
                context.reader().maxDoc(), leadCost, boost, pointValues, docValues);
          }

          @Override
          public long cost() {
            return docValues.cost();
          }
        };
      }
    };
  }

  private class DistanceScorer extends Scorer {

    private final int maxDoc;
    private DocIdSetIterator it;
    private int doc = -1;
    private final long leadCost;
    private final float boost;
    private final PointValues pointValues;
    private final NumericDocValues docValues;
    private long maxDistance = Long.MAX_VALUE;

    private int currentSkipInterval = MIN_SKIP_INTERVAL;
    // helps to be conservative about increasing the sampling interval
    private int tryUpdateFailCount = 0;

    protected DistanceScorer(
        int maxDoc,
        long leadCost,
        float boost,
        PointValues pointValues,
        NumericDocValues docValues) {
      this.maxDoc = maxDoc;
      this.leadCost = leadCost;
      this.boost = boost;
      this.pointValues = pointValues;
      this.docValues = docValues;
      // initially use doc values in order to iterate all documents that have
      // a value for this field
      this.it = docValues;
    }

    @Override
    public int docID() {
      return doc;
    }

    private float score(long distance) {
      return (float) (boost * (pivotDistance / (pivotDistance + (double) distance)));
    }

    /**
     * Inverting the score computation is very hard due to all potential rounding errors, so we
     * binary search the maximum distance.
     */
    private long computeMaxDistance(float minScore, long previousMaxDistance) {
      assert score(0) >= minScore;
      if (score(previousMaxDistance) >= minScore) {
        // minScore did not decrease enough to require an update to the max distance
        return previousMaxDistance;
      }
      assert score(previousMaxDistance) < minScore;
      long min = 0, max = previousMaxDistance;
      // invariant: score(min) >= minScore && score(max) < minScore
      while (max - min > 1) {
        long mid = (min + max) >>> 1;
        float score = score(mid);
        if (score >= minScore) {
          min = mid;
        } else {
          max = mid;
        }
      }
      assert score(min) >= minScore;
      assert min == Long.MAX_VALUE || score(min + 1) < minScore;
      return min;
    }

    @Override
    public float score() throws IOException {
      if (docValues.advanceExact(docID()) == false) {
        return 0;
      }
      long v = docValues.longValue();
      // note: distance is unsigned
      long distance = Math.max(v, origin) - Math.min(v, origin);
      if (distance < 0) {
        // underflow
        // treat distances that are greater than MAX_VALUE as MAX_VALUE
        distance = Long.MAX_VALUE;
      }
      return score(distance);
    }

    @Override
    public DocIdSetIterator iterator() {
      // add indirection so that if 'it' is updated then it will
      // be taken into account
      return new DocIdSetIterator() {

        @Override
        public int nextDoc() throws IOException {
          return doc = it.nextDoc();
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return it.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return doc = it.advance(target);
        }
      };
    }

    @Override
    public float getMaxScore(int upTo) {
      return boost;
    }

    private int setMinCompetitiveScoreCounter = 0;

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (minScore > boost) {
        it = DocIdSetIterator.empty();
        return;
      }

      // Start sampling if we get called too much
      setMinCompetitiveScoreCounter++;
      if (setMinCompetitiveScoreCounter > 256
          && (setMinCompetitiveScoreCounter & (currentSkipInterval - 1))
              != currentSkipInterval - 1) {
        return;
      }

      long previousMaxDistance = maxDistance;
      maxDistance = computeMaxDistance(minScore, maxDistance);
      if (maxDistance == previousMaxDistance) {
        // nothing to update
        return;
      }
      long minValue = origin - maxDistance;
      if (minValue > origin) {
        // underflow
        minValue = Long.MIN_VALUE;
      }
      long maxValue = origin + maxDistance;
      if (maxValue < origin) {
        // overflow
        maxValue = Long.MAX_VALUE;
      }
      long min = minValue;
      long max = maxValue;

      DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
      final int doc = docID();
      IntersectVisitor visitor =
          new IntersectVisitor() {

            DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
              adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
              if (docID <= doc) {
                // Already visited or skipped
                return;
              }
              adder.add(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              if (docID <= doc) {
                // Already visited or skipped
                return;
              }
              long docValue = NumericUtils.sortableBytesToLong(packedValue, 0);
              if (docValue < min || docValue > max) {
                // Doc's value is too low, in this dimension
                return;
              }

              // Doc is in-bounds
              adder.add(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }

            @Override
            public void visit(IntsRef ref) {
              for (int i = 0; i < ref.length; ++i) {
                visit(ref.ints[ref.offset + i]);
              }
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              long minDocValue = NumericUtils.sortableBytesToLong(minPackedValue, 0);
              long maxDocValue = NumericUtils.sortableBytesToLong(maxPackedValue, 0);

              if (minDocValue > max || maxDocValue < min) {
                return Relation.CELL_OUTSIDE_QUERY;
              }

              if (minDocValue < min || maxDocValue > max) {
                return Relation.CELL_CROSSES_QUERY;
              }

              return Relation.CELL_INSIDE_QUERY;
            }
          };

      final long currentQueryCost = Math.min(leadCost, it.cost());
      // TODO: what is the right factor compared to the current disi? Is 8 optimal?
      final long threshold = currentQueryCost >>> 3;
      if (PointValues.isEstimatedPointCountGreaterThanOrEqualTo(
          visitor, pointValues.getPointTree(), threshold)) {
        // the new range is not selective enough to be worth materializing
        updateSkipInterval(false);
        return;
      }
      pointValues.intersect(visitor);
      it = result.build().iterator();
      updateSkipInterval(true);
    }

    private void updateSkipInterval(boolean success) {
      if (setMinCompetitiveScoreCounter > 256) {
        if (success) {
          currentSkipInterval = Math.max(currentSkipInterval / 2, MIN_SKIP_INTERVAL);
          tryUpdateFailCount = 0;
        } else {
          if (tryUpdateFailCount >= 3) {
            currentSkipInterval = Math.min(currentSkipInterval * 2, MAX_SKIP_INTERVAL);
            tryUpdateFailCount = 0;
          } else {
            tryUpdateFailCount++;
          }
        }
      }
    }
  }
}
