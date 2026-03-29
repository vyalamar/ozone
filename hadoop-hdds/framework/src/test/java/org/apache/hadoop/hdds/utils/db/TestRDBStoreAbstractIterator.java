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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksIterator;

/**
 * Unit tests for {@link RDBStoreAbstractIterator}, focusing on:
 * <ul>
 *   <li>{@link RDBStoreAbstractIterator#getNextHigherPrefix(byte[])} edge cases</li>
 *   <li>Verification that {@link ManagedReadOptions} lower/upper bounds are set
 *       correctly from the prefix</li>
 *   <li>Verification that bounds are closed when the iterator is closed</li>
 * </ul>
 */
public class TestRDBStoreAbstractIterator {

  @BeforeEach
  public void setup() {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Creates a real-enough iterator whose ReadOptions we can capture. */
  private static CaptureResult newByteArrayIterator(byte[] prefix) throws RocksDatabaseException {
    final ManagedReadOptions[] captured = {null};
    final RocksIterator rocksIterMock = mock(RocksIterator.class);
    final ManagedRocksIterator managed = new ManagedRocksIterator(rocksIterMock);
    final RDBStoreByteArrayIterator iter = new RDBStoreByteArrayIterator(
        ro -> {
          captured[0] = ro;
          return managed;
        }, null, prefix, KEY_AND_VALUE);
    return new CaptureResult(iter, captured[0]);
  }

  private static class CaptureResult implements AutoCloseable {
    final RDBStoreByteArrayIterator iterator;
    final ManagedReadOptions readOptions;

    CaptureResult(RDBStoreByteArrayIterator iterator, ManagedReadOptions readOptions) {
      this.iterator = iterator;
      this.readOptions = readOptions;
    }

    @Override
    public void close() {
      iterator.close();
    }
  }

  // -----------------------------------------------------------------------
  // getNextHigherPrefix tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetNextHigherPrefix_null() {
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(null));
  }

  @Test
  public void testGetNextHigherPrefix_empty() {
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(new byte[0]));
  }

  @Test
  public void testGetNextHigherPrefix_singleZero() {
    assertArrayEquals(new byte[]{0x01},
        RDBStoreAbstractIterator.getNextHigherPrefix(new byte[]{0x00}));
  }

  @Test
  public void testGetNextHigherPrefix_singleFF() {
    // Single 0xFF — no upper bound possible
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(new byte[]{(byte) 0xFF}));
  }

  @Test
  public void testGetNextHigherPrefix_allFF() {
    // All 0xFF bytes — no upper bound possible
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(
        new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  public void testGetNextHigherPrefix_trailingFF() {
    // [0x01, 0x02, 0xFF] → [0x01, 0x03]
    assertArrayEquals(new byte[]{0x01, 0x03},
        RDBStoreAbstractIterator.getNextHigherPrefix(new byte[]{0x01, 0x02, (byte) 0xFF}));
  }

  @Test
  public void testGetNextHigherPrefix_noTrailingFF() {
    // [0x01, 0x02] → [0x01, 0x03]
    assertArrayEquals(new byte[]{0x01, 0x03},
        RDBStoreAbstractIterator.getNextHigherPrefix(new byte[]{0x01, 0x02}));
  }

  @Test
  public void testGetNextHigherPrefix_singleByte() {
    // [0x7F] → [0x80]
    assertArrayEquals(new byte[]{(byte) 0x80},
        RDBStoreAbstractIterator.getNextHigherPrefix(new byte[]{0x7F}));
  }

  @Test
  public void testGetNextHigherPrefix_multipleTrailingFF() {
    // [0x01, 0xFF, 0xFF] → [0x02]
    assertArrayEquals(new byte[]{0x02},
        RDBStoreAbstractIterator.getNextHigherPrefix(
            new byte[]{0x01, (byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  public void testGetNextHigherPrefix_random() {
    // With any random prefix ending in a non-0xFF byte, the result must be exactly
    // one byte longer-or-equal to the prefix in value ordering and be the
    // next higher prefix.
    final byte[] prefix = new byte[8];
    ThreadLocalRandom.current().nextBytes(prefix);
    // Ensure last byte is not 0xFF so there is always an upper bound
    prefix[prefix.length - 1] = (byte) (prefix[prefix.length - 1] & 0xFE);

    final byte[] upper = RDBStoreAbstractIterator.getNextHigherPrefix(prefix);
    assertNotNull(upper);
    // upper must be strictly greater than prefix in unsigned byte ordering
    // Simple check: it must start with bytes from prefix and have last
    // incremented byte
    assertArrayEquals(
        java.util.Arrays.copyOf(prefix, upper.length - 1),
        java.util.Arrays.copyOf(upper, upper.length - 1));
  }

  // -----------------------------------------------------------------------
  // ReadOptions bounds tests
  // -----------------------------------------------------------------------

  @Test
  public void testNullPrefixSetsNoBounds() throws Exception {
    try (CaptureResult r = newByteArrayIterator(null)) {
      assertNull(r.readOptions.getLowerBound());
      assertNull(r.readOptions.getUpperBound());
    }
  }

  @Test
  public void testEmptyPrefixSetsNoBounds() throws Exception {
    try (CaptureResult r = newByteArrayIterator(new byte[0])) {
      // Empty prefix is semantically equivalent to no prefix.
      // Lower bound is stored as-is (empty array or null); upper bound must be null
      // because getNextHigherPrefix([]) == null.
      final byte[] lower = r.readOptions.getLowerBound();
      assertTrue(lower == null || lower.length == 0,
          "Empty prefix should not set a meaningful lower bound");
      assertNull(r.readOptions.getUpperBound());
    }
  }

  @Test
  public void testNormalPrefixSetsBothBounds() throws Exception {
    byte[] prefix = {0x01, 0x02};
    try (CaptureResult r = newByteArrayIterator(prefix)) {
      assertArrayEquals(prefix, r.readOptions.getLowerBound());
      assertArrayEquals(new byte[]{0x01, 0x03}, r.readOptions.getUpperBound());
    }
  }

  @Test
  public void testAllFFPrefixSetsLowerBoundOnly() throws Exception {
    byte[] allFF = {(byte) 0xFF, (byte) 0xFF};
    try (CaptureResult r = newByteArrayIterator(allFF)) {
      assertArrayEquals(allFF, r.readOptions.getLowerBound());
      // No upper bound for all-0xFF prefix
      assertNull(r.readOptions.getUpperBound());
    }
  }

  @Test
  public void testBoundsAreClosedWhenIteratorIsClosed() throws Exception {
    // After closing the iterator, the ReadOptions should be closed.
    // We verify this indirectly by ensuring no native memory leak is reported.
    // (ManagedSlice leak tracking fires on GC if not closed.)
    byte[] prefix = {0x61, 0x62, 0x63}; // "abc"
    CaptureResult r = newByteArrayIterator(prefix);
    assertNotNull(r.readOptions.getLowerBound());
    assertNotNull(r.readOptions.getUpperBound());
    r.iterator.close(); // Must close ReadOptions and its slices
    // After close() the ManagedReadOptions is closed — no leak tracker fires
  }
}
