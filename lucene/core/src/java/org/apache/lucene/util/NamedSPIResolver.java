package org.apache.lucene.util;

public interface NamedSPIResolver {
  <S> Iterable<S> resolve(Class<S> clazz, ClassLoader classLoader);
}
