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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestIndexOrDocValuesQuery extends LuceneTestCase {

  public void testUseIndexForSelectiveQueries() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                // relies on costs and PointValues.estimateCost so we need the default codec
                .setCodec(TestUtil.getDefaultCodec()));
    for (int i = 0; i < 2000; ++i) {
      Document doc = new Document();
      if (i == 42) {
        doc.add(new StringField("f1", "bar", Store.NO));
        doc.add(new LongPoint("f2", 42L));
        doc.add(new NumericDocValuesField("f2", 42L));
      } else if (i == 100) {
        doc.add(new StringField("f1", "foo", Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      } else {
        doc.add(new StringField("f1", "bar", Store.NO));
        doc.add(new LongPoint("f2", 2L));
        doc.add(new NumericDocValuesField("f2", 2L));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null);

    // The term query is more selective, so the IndexOrDocValuesQuery should use doc values
    final Query q1 =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f1", "foo")), Occur.MUST)
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 2),
                    NumericDocValuesField.newSlowRangeQuery("f2", 2L, 2L)),
                Occur.MUST)
            .build();
    QueryUtils.check(random(), q1, searcher);

    final Weight w1 = searcher.createWeight(searcher.rewrite(q1), ScoreMode.COMPLETE, 1);
    final Scorer s1 = w1.scorer(searcher.getIndexReader().leaves().get(0));
    assertNotNull(s1.twoPhaseIterator()); // means we use doc values

    // The term query is less selective, so the IndexOrDocValuesQuery should use points
    final Query q2 =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f1", "bar")), Occur.MUST)
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 42),
                    NumericDocValuesField.newSlowRangeQuery("f2", 42L, 42L)),
                Occur.MUST)
            .build();
    QueryUtils.check(random(), q2, searcher);

    final Weight w2 = searcher.createWeight(searcher.rewrite(q2), ScoreMode.COMPLETE, 1);
    final Scorer s2 = w2.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(s2.twoPhaseIterator()); // means we use points

    reader.close();
    w.close();
    dir.close();
  }

  public void testUseIndexForSelectiveMultiValueQueries() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                // relies on costs and PointValues.estimateCost so we need the default codec
                .setCodec(TestUtil.getDefaultCodec()));
    final int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i < numDocs / 2) {
        doc.add(new StringField("f1", "bar", Store.NO));
        for (int j = 0; j < 500; j++) {
          doc.add(new LongField("f2", 42L, Store.NO));
        }
      } else if (i == numDocs / 2) {
        doc.add(new StringField("f1", "foo", Store.NO));
        doc.add(new LongField("f2", 2L, Store.NO));
      } else {
        doc.add(new StringField("f1", "bar", Store.NO));
        for (int j = 0; j < 100; j++) {
          doc.add(new LongField("f2", 2L, Store.NO));
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null);

    // The term query is less selective, so the IndexOrDocValuesQuery should use points
    final Query q1 =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f1", "bar")), Occur.MUST)
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 2),
                    SortedNumericDocValuesField.newSlowRangeQuery("f2", 2L, 2L)),
                Occur.MUST)
            .build();
    QueryUtils.check(random(), q1, searcher);

    final Weight w1 = searcher.createWeight(searcher.rewrite(q1), ScoreMode.COMPLETE, 1);
    final Scorer s1 = w1.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(s1.twoPhaseIterator()); // means we use points

    // The term query is less selective, so the IndexOrDocValuesQuery should use points
    final Query q2 =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f1", "bar")), Occur.MUST)
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 42),
                    SortedNumericDocValuesField.newSlowRangeQuery("f2", 42, 42L)),
                Occur.MUST)
            .build();
    QueryUtils.check(random(), q2, searcher);

    final Weight w2 = searcher.createWeight(searcher.rewrite(q2), ScoreMode.COMPLETE, 1);
    final Scorer s2 = w2.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(s2.twoPhaseIterator()); // means we use points

    // The term query is more selective, so the IndexOrDocValuesQuery should use doc values
    final Query q3 =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f1", "foo")), Occur.MUST)
            .add(
                new IndexOrDocValuesQuery(
                    LongPoint.newExactQuery("f2", 42),
                    SortedNumericDocValuesField.newSlowRangeQuery("f2", 42, 42L)),
                Occur.MUST)
            .build();
    QueryUtils.check(random(), q3, searcher);

    final Weight w3 = searcher.createWeight(searcher.rewrite(q3), ScoreMode.COMPLETE, 1);
    final Scorer s3 = w3.scorer(searcher.getIndexReader().leaves().get(0));
    assertNotNull(s3.twoPhaseIterator()); // means we use doc values

    reader.close();
    w.close();
    dir.close();
  }

  // Weight#count is delegated to the inner weight
  public void testQueryMatchesCount() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(
            dir,
            newIndexWriterConfig()
                // relies on costs and PointValues.estimateCost so we need the default codec
                .setCodec(TestUtil.getDefaultCodec()));
    final int numDocs = random().nextInt(5000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new LongPoint("f2", 42L));
      doc.add(new SortedNumericDocValuesField("f2", 42L));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);

    final IndexOrDocValuesQuery query =
        new IndexOrDocValuesQuery(
            LongPoint.newExactQuery("f2", 42),
            SortedNumericDocValuesField.newSlowRangeQuery("f2", 42, 42L));
    QueryUtils.check(random(), query, searcher);

    final int searchCount = searcher.count(query);
    final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1);
    int weightCount = 0;
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      weightCount += weight.count(leafReaderContext);
    }
    assertEquals(searchCount, weightCount);

    reader.close();
    w.close();
    dir.close();
  }

  public void testQueryMatchesAllOrNone() throws Exception {
    var config = newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config)) {
      final int numDocs = TestUtil.nextInt(random(), 1, 100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        doc.add(new LongPoint("f2", i));
        doc.add(new SortedNumericDocValuesField("f2", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);

      try (IndexReader reader = DirectoryReader.open(w)) {
        IndexSearcher searcher = newSearcher(reader);

        // range with all docs
        IndexOrDocValuesQuery query =
            new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("f2", 0, numDocs),
                SortedNumericDocValuesField.newSlowRangeQuery("f2", 0, numDocs));
        QueryUtils.check(random(), query, searcher);
        assertSame(MatchAllDocsQuery.class, query.rewrite(searcher).getClass());

        // range with no docs
        query =
            new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("f2", numDocs + 1, numDocs + 200),
                SortedNumericDocValuesField.newSlowRangeQuery("f2", numDocs + 1, numDocs + 200));
        QueryUtils.check(random(), query, searcher);
        assertSame(MatchNoDocsQuery.class, query.rewrite(searcher).getClass());
      }
    }
  }
}
