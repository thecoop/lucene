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
package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

public class TopNQueue<T> {

  private final Comparator<T> comp;
  private PriorityQueue<T> queue;

  public TopNQueue(Comparator<T> comparator, int maxSize) {
    this.comp = comparator;
    this.queue =
        new PriorityQueue<>(maxSize) {
          @Override
          protected boolean lessThan(T a, T b) {
            return comp.compare(a, b) < 0;
          }
        };
  }

  public TopNQueue(Comparator<T> comparator, int maxSize, Supplier<T> sentinelObjectSupplier) {
    this.comp = comparator;
    this.queue =
        new PriorityQueue<>(maxSize, sentinelObjectSupplier) {
          @Override
          protected boolean lessThan(T a, T b) {
            return comp.compare(a, b) < 0;
          }
        };
  }

  public int size() {
    if (queue == null) throw new IllegalStateException("Queue has been drained");
    return queue.size();
  }

  public T top() {
    if (queue == null) throw new IllegalStateException("Queue has been drained");
    return queue.top();
  }

  public T updateTop() {
    if (queue == null) throw new IllegalStateException("Queue has been drained");
    return queue.updateTop();
  }

  public T add(T element) {
    if (queue == null) throw new IllegalStateException("Queue has been drained");
    return queue.add(element);
  }

  public T insertWithOverflow(T element) {
    if (queue == null) throw new IllegalStateException("Queue has been drained");
    return queue.insertWithOverflow(element);
  }

  public List<T> drainToSortedList() {
    return drainToSortedList(comp);
  }

  public List<T> drainToSortedList(Comparator<T> comparator) {
    @SuppressWarnings("unchecked")
    T[] array = (T[]) queue.getHeapArray();
    int endOffset = queue.size() + 1;
    ArrayUtil.introSort(array, 1, endOffset, comparator);
    queue = null;
    return Arrays.asList(array).subList(1, endOffset);
  }
}
