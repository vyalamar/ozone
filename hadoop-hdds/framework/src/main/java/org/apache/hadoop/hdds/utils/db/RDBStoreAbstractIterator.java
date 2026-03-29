/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link Table.KeyValueIterator} to iterate raw {@link Table.KeyValue}s.
 *
 * @param <RAW> the raw type.
 */
abstract class RDBStoreAbstractIterator<RAW>
    implements Table.KeyValueIterator<RAW, RAW> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStoreAbstractIterator.class);

  private final ManagedRocksIterator rocksDBIterator;
  private final RDBTable rocksDBTable;
  private Table.KeyValue<RAW, RAW> currentEntry;
  private final IteratorType type;
  private final ManagedReadOptions readOptions;
  private final AtomicBoolean isIteratorClosed = new AtomicBoolean(false);

  /**
   * Constructor for RDBStoreAbstractIterator using a prefix.
   */
  RDBStoreAbstractIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> iteratorSupplier,
      RDBTable table, byte[] prefix, IteratorType type) throws RocksDatabaseException {
    this(iteratorSupplier, table, prefix, getNextHigherPrefix(prefix), type);
  }

  /**
   * Constructor for RDBStoreAbstractIterator using explicit bounds.
   */
  RDBStoreAbstractIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> iteratorSupplier,
      RDBTable table, byte[] lowerBound, byte[] upperBound, IteratorType type) throws RocksDatabaseException {
    this.rocksDBTable = table;
    this.type = type;
    this.readOptions = new ManagedReadOptions(lowerBound, upperBound);
    try {
      this.rocksDBIterator = iteratorSupplier.apply(readOptions);
    } catch (RocksDatabaseException e) {
      readOptions.close();
      throw e;
    }
  }

  /**
   * Computes the exclusive upper bound for a given prefix.
   * Walks bytes from end, finds first non-0xFF byte, increments it, and truncates.
   * Returns null if prefix is null/empty or all bytes are 0xFF (no upper bound).
   */
  static byte[] getNextHigherPrefix(byte[] prefix) {
    if (prefix == null || prefix.length == 0) {
      return null;
    }
    for (int i = prefix.length - 1; i >= 0; i--) {
      if ((prefix[i] & 0xFF) != 0xFF) {
        byte[] result = Arrays.copyOf(prefix, i + 1);
        result[i]++;
        return result;
      }
    }
    return null; // all bytes are 0xFF, no upper bound
  }

  IteratorType getType() {
    return type;
  }

  /** @return the {@link Table.KeyValue} for the current entry. */
  abstract Table.KeyValue<RAW, RAW> getKeyValue();

  /** Seek to the given key. */
  abstract void seek0(RAW key);

  /** Delete the given key. */
  abstract void delete(RAW key) throws RocksDatabaseException;

  final ManagedRocksIterator getRocksDBIterator() {
    return rocksDBIterator;
  }

  final RDBTable getRocksDBTable() {
    return rocksDBTable;
  }

  @Override
  public final void forEachRemaining(
      Consumer<? super Table.KeyValue<RAW, RAW>> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  private boolean isDbClosed() {
    return rocksDBTable != null && rocksDBTable.isClosed();
  }

  private void setCurrentEntry() {
    if (rocksDBIterator.get().isValid()) {
      currentEntry = getKeyValue();
    } else {
      currentEntry = null;
    }
  }

  @Override
  public final boolean hasNext() {
    if (isDbClosed()) {
      return false;
    }
    return rocksDBIterator.get().isValid();
  }

  @Override
  public final Table.KeyValue<RAW, RAW> next() {
    setCurrentEntry();
    if (currentEntry != null) {
      rocksDBIterator.get().next();
      return currentEntry;
    }
    throw new NoSuchElementException("RocksDB Store has no more elements");
  }

  @Override
  public final void seekToFirst() {
    rocksDBIterator.get().seekToFirst();
    setCurrentEntry();
  }

  @Override
  public final void seekToLast() {
    rocksDBIterator.get().seekToLast();
    setCurrentEntry();
  }

  @Override
  public final Table.KeyValue<RAW, RAW> seek(RAW key) {
    seek0(key);
    setCurrentEntry();
    return currentEntry;
  }

  @Override
  public final void removeFromDB() throws RocksDatabaseException, CodecException {
    if (rocksDBTable == null) {
      throw new UnsupportedOperationException("remove");
    }
    if (currentEntry != null) {
      delete(currentEntry.getKey());
    } else {
      LOG.info("Failed to removeFromDB: currentEntry == null");
    }
  }

  @Override
  public void close() {
    if (isIteratorClosed.compareAndSet(false, true)) {
      try {
        rocksDBIterator.close();
      } finally {
        readOptions.close();
      }
    }
  }
}
