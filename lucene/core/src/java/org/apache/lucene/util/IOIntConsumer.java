package org.apache.lucene.util;

import java.io.IOException;

/**
 * An IO operation with a single int input that may throw an IOException.
 *
 * @see java.util.function.IntConsumer
 */
@FunctionalInterface
public interface IOIntConsumer {
  /**
   * Performs this operation on the given argument.
   *
   * @param value the value to accept
   * @throws IOException if producing the result throws an {@link IOException}
   */
  void accept(int value) throws IOException;
}
