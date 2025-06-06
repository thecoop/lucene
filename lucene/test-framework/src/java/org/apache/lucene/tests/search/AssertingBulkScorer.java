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
package org.apache.lucene.tests.search;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Random;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;

/** Wraps a Scorer with additional checks */
public final class AssertingBulkScorer extends BulkScorer {

  public static BulkScorer wrap(Random random, BulkScorer other, int maxDoc) {
    if (other == null || other instanceof AssertingBulkScorer) {
      return other;
    }
    return new AssertingBulkScorer(random, other, maxDoc);
  }

  final Random random;
  final BulkScorer in;
  final int maxDoc;
  int max = 0;

  private AssertingBulkScorer(Random random, BulkScorer in, int maxDoc) {
    this.random = random;
    this.in = in;
    this.maxDoc = maxDoc;
  }

  public BulkScorer getIn() {
    return in;
  }

  @Override
  public long cost() {
    return in.cost();
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, final int max)
      throws IOException {
    assert min >= this.max
        : "Scoring backward: min=" + min + " while previous max was max=" + this.max;
    assert min <= max : "max must be greater than min, got min=" + min + ", and max=" + max;
    this.max = max;
    collector = new AssertingLeafCollector(collector, min, max);
    int next = min;
    do {
      final int upTo;
      if (random.nextBoolean()) {
        upTo = max;
      } else {
        final long interval;
        if (random.nextInt(100) <= 5) {
          interval = 1 + random.nextInt(10);
        } else {
          interval = 1 + random.nextInt(random.nextBoolean() ? 100 : 5000);
        }
        upTo = Math.toIntExact(Math.min(next + interval, max));
      }
      next = in.score(new AssertingLeafCollector(collector, next, upTo), acceptDocs, next, upTo);
    } while (next < max);

    if (max >= maxDoc || next >= maxDoc) {
      assert next == DocIdSetIterator.NO_MORE_DOCS;
      return DocIdSetIterator.NO_MORE_DOCS;
    } else {
      return RandomNumbers.randomIntBetween(random, max, next);
    }
  }

  @Override
  public String toString() {
    return "AssertingBulkScorer(" + in + ")";
  }
}
