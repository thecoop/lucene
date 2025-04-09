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
package org.apache.lucene.search.suggest.document;

import java.util.List;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;
import org.apache.lucene.util.TopNQueue;

/**
 * Bounded priority queue for {@link SuggestScoreDoc}s. Priority is based on {@link
 * SuggestScoreDoc#score} and tie is broken by {@link SuggestScoreDoc#doc}
 */
final class SuggestScoreDocPriorityQueue extends TopNQueue<SuggestScoreDoc> {

  private static int compare(SuggestScoreDoc a, SuggestScoreDoc b) {
    int res = Float.compare(a.score, b.score);
    if (res == 0) {
      // tie break by completion key
      res = Lookup.CHARSEQUENCE_COMPARATOR.compare(b.key, a.key);
    }
    if (res == 0) {
      res = Integer.compare(b.doc, a.doc);
    }
    return res;
  }

  /** Creates a new priority queue of the specified size. */
  public SuggestScoreDocPriorityQueue(int size) {
    super(SuggestScoreDocPriorityQueue::compare, size);
  }

  /** Returns the top N results in descending order. */
  public List<SuggestScoreDoc> getResults() {
    return drainToSortedListReversed();
  }
}
