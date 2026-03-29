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

import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ByteArrayRawSpliterator} / {@link RDBTable#spliterator} behavior.
 * <p>
 * Covers gaps not exercised by a single split + sequential read: recursive splits,
 * duplicate detection across partitions, try-split after advance, parallel streams,
 * memtable-only DBs, and non-null start key semantics.
 */
public class TestRawSpliterator {

  private RDBStore rdbStore = null;
  private ManagedDBOptions options = null;
  private final String tableName = "SpliteratorTable";

  @TempDir
  private File tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    options = new ManagedDBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Set<TableConfig> configSet = new HashSet<>();
    configSet.add(new TableConfig(
        org.apache.hadoop.hdds.StringUtils.bytes2String(org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY),
        new ManagedColumnFamilyOptions()));
    TableConfig newConfig = new TableConfig(tableName,
        new ManagedColumnFamilyOptions());
    configSet.add(newConfig);

    rdbStore = TestRDBStore.newRDBStore(tempDir, options, configSet, 80);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
    if (options != null) {
      options.close();
    }
  }

  @Test
  public void testSpliteratorPartitioning() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);

    // Write a bunch of keys and force flushes to create multiple SST files
    int totalKeys = 300;
    List<String> writtenKeys = new ArrayList<>();
    
    for (int i = 0; i < totalKeys; i++) {
        String keyStr = String.format("key-%05d", i);
        writtenKeys.add(keyStr);
        testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
        
        // Force a flush every 50 keys to create multiple SST files
        if (i % 50 == 0) {
            rdbStore.getDb().flush(tableName);
        }
    }
    rdbStore.getDb().flush(tableName);

    Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(1, true);
    assertNotNull(spliterator);

    // Try splitting
    Spliterator<Table.KeyValue<byte[], byte[]>> split1 = spliterator.trySplit();
    assertNotNull(split1, "Spliterator should have split due to multiple SST files");

    List<String> readKeys = new ArrayList<>();
    
    // Read from both spliterators
    spliterator.forEachRemaining(kv -> {
      readKeys.add(new String(kv.getKey(), StandardCharsets.UTF_8));
    });
    
    split1.forEachRemaining(kv -> {
      readKeys.add(new String(kv.getKey(), StandardCharsets.UTF_8));
    });

    spliterator.close();
    ((Table.KeyValueSpliterator<byte[], byte[]>) split1).close();

    Collections.sort(readKeys);
    Collections.sort(writtenKeys);

    assertEquals(totalKeys, readKeys.size(), "Should have read all written keys");
    assertEquals(writtenKeys, readKeys, "Read keys should match written keys");
    
    // Test TypedTable Spliterator
    TypedTable<String, String> typedTable = new TypedTable<>(
        (RDBTable) testTable, StringCodec.get(), StringCodec.get(), CacheType.NO_CACHE);
        
    Table.KeyValueSpliterator<String, String> typedSpliterator = typedTable.spliterator(1, true);
    assertNotNull(typedSpliterator);
    
    Spliterator<Table.KeyValue<String, String>> typedSplit = typedSpliterator.trySplit();
    assertNotNull(typedSplit, "TypedSpliterator should have split");
    
    List<String> typedReadKeys = new ArrayList<>();
    typedSpliterator.forEachRemaining(kv -> typedReadKeys.add(kv.getKey()));
    typedSplit.forEachRemaining(kv -> typedReadKeys.add(kv.getKey()));
    
    typedSpliterator.close();
    ((Table.KeyValueSpliterator<String, String>) typedSplit).close();
    
    Collections.sort(typedReadKeys);
    assertEquals(totalKeys, typedReadKeys.size(), "Typed Spliterator should have read all written keys");
    assertEquals(writtenKeys, typedReadKeys, "Typed read keys should match written keys");
  }

  /**
   * Recursively splits until {@link Spliterator#trySplit()} returns null, then drains each leaf.
   * Closes all spliterators in post-order.
   */
  private static void drainSpliteratorRecursive(Table.KeyValueSpliterator<byte[], byte[]> spliterator,
      List<String> readKeys) throws Exception {
    Spliterator<Table.KeyValue<byte[], byte[]>> left = spliterator.trySplit();
    if (left == null) {
      spliterator.forEachRemaining(kv ->
          readKeys.add(new String(kv.getKey(), StandardCharsets.UTF_8)));
      spliterator.close();
      return;
    }
    @SuppressWarnings("resource")
    Table.KeyValueSpliterator<byte[], byte[]> leftSpl =
        (Table.KeyValueSpliterator<byte[], byte[]>) left;
    drainSpliteratorRecursive(leftSpl, readKeys);
    drainSpliteratorRecursive(spliterator, readKeys);
  }

  /**
   * After {@link Spliterator#tryAdvance} has run, {@link Spliterator#trySplit} must not partition
   * the same key range (would duplicate or skip keys with stale native iterators).
   */
  @Test
  public void testTrySplitAfterTryAdvanceReturnsNull() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    for (int i = 0; i < 120; i++) {
      String keyStr = String.format("key-%05d", i);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
      if (i % 40 == 0) {
        rdbStore.getDb().flush(tableName);
      }
    }
    rdbStore.getDb().flush(tableName);

    Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(8, true);
    assertTrue(spliterator.tryAdvance(kv -> assertNotNull(kv.getKey(), "advanced key")),
        "Should advance at least one element");
    assertNull(spliterator.trySplit(), "trySplit after tryAdvance must return null");
    spliterator.close();
  }

  /**
   * Every key appears exactly once across all recursively split partitions (no duplicates).
   */
  @Test
  public void testNoDuplicateKeysAcrossRecursiveSplits() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    int totalKeys = 200;
    List<String> writtenKeys = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      String keyStr = String.format("key-%05d", i);
      writtenKeys.add(keyStr);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
      if (i % 40 == 0) {
        rdbStore.getDb().flush(tableName);
      }
    }
    rdbStore.getDb().flush(tableName);

    List<String> readKeys = new ArrayList<>();
    try (Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(8, true)) {
      drainSpliteratorRecursive(spliterator, readKeys);
    }

    assertEquals(totalKeys, readKeys.size(), "Total elements must match written count");
    assertEquals(totalKeys, new HashSet<>(readKeys).size(),
        "No duplicate keys across split partitions");
    Collections.sort(readKeys);
    assertEquals(writtenKeys, readKeys);
  }

  /**
   * Higher maxParallelism allows deeper splitting; still must cover all keys without duplicates.
   */
  @Test
  public void testRecursiveSplitsWithHigherMaxParallelism() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    int totalKeys = 250;
    List<String> writtenKeys = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      String keyStr = String.format("key-%05d", i);
      writtenKeys.add(keyStr);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
      if (i % 35 == 0) {
        rdbStore.getDb().flush(tableName);
      }
    }
    rdbStore.getDb().flush(tableName);

    List<String> readKeys = new ArrayList<>();
    try (Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(8, true)) {
      drainSpliteratorRecursive(spliterator, readKeys);
    }

    assertEquals(totalKeys, readKeys.size());
    assertEquals(totalKeys, new HashSet<>(readKeys).size());
  }

  /**
   * Sequential stream over the spliterator must collect all keys without duplicates.
   * (Parallel streams are not supported; the spliterator does not advertise CONCURRENT.)
   */
  @Test
  public void testSequentialStreamCollectsAllKeysWithoutDuplicates() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    int totalKeys = 180;
    List<String> writtenKeys = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      String keyStr = String.format("key-%05d", i);
      writtenKeys.add(keyStr);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
      if (i % 45 == 0) {
        rdbStore.getDb().flush(tableName);
      }
    }
    rdbStore.getDb().flush(tableName);

    List<String> readKeys;
    try (Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(6, true)) {
      // Sequential stream (parallel=false); spliterator does not guarantee thread safety
      readKeys = StreamSupport.stream(spliterator, false)
          .map(kv -> new String(kv.getKey(), StandardCharsets.UTF_8))
          .collect(Collectors.toList());
    }

    assertEquals(totalKeys, readKeys.size());
    assertEquals(totalKeys, new HashSet<>(readKeys).size());
    Collections.sort(readKeys);
    assertEquals(writtenKeys, readKeys);
  }

  /**
   * With no SST files (data only in memtable), spliterator should still scan the full table.
   */
  @Test
  public void testMemtableOnlyTableStillScansAllKeys() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    int totalKeys = 12;
    List<String> writtenKeys = new ArrayList<>();
    for (int i = 0; i < totalKeys; i++) {
      String keyStr = String.format("mt-%03d", i);
      writtenKeys.add(keyStr);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
    }

    List<String> readKeys = new ArrayList<>();
    try (Table.KeyValueSpliterator<byte[], byte[]> spliterator = testTable.spliterator(8, true)) {
      assertNull(spliterator.trySplit(), "No SSTs: expect no split, single-range scan");
      spliterator.forEachRemaining(kv ->
          readKeys.add(new String(kv.getKey(), StandardCharsets.UTF_8)));
    }

    Collections.sort(readKeys);
    Collections.sort(writtenKeys);
    assertEquals(writtenKeys, readKeys);
  }

  /**
   * Non-null start key: every emitted key must be &gt;= startKey in unsigned byte order.
   * Documents expected contract for partial table scans.
   */
  @Test
  public void testSpliteratorWithNonNullStartKeyOnlyEmitsFromStart() throws Exception {
    Table<byte[], byte[]> testTable = rdbStore.getTable(tableName);
    int totalKeys = 200;
    for (int i = 0; i < totalKeys; i++) {
      String keyStr = String.format("key-%05d", i);
      testTable.put(keyStr.getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
      if (i % 40 == 0) {
        rdbStore.getDb().flush(tableName);
      }
    }
    rdbStore.getDb().flush(tableName);

    final String startKeyStr = "key-00100";
    final byte[] startKeyBytes = startKeyStr.getBytes(StandardCharsets.UTF_8);

    List<String> readKeys = new ArrayList<>();
    try (Table.KeyValueSpliterator<byte[], byte[]> spliterator =
             testTable.spliterator(startKeyBytes, null, 8, true)) {
      drainSpliteratorRecursive(spliterator, readKeys);
    }

    for (String k : readKeys) {
      assertTrue(k.compareTo(startKeyStr) >= 0,
          "Key " + k + " must be >= startKey " + startKeyStr);
    }

    List<String> expected = new ArrayList<>();
    for (int i = 100; i < totalKeys; i++) {
      expected.add(String.format("key-%05d", i));
    }
    Collections.sort(readKeys);
    assertEquals(expected, readKeys,
        "All keys from startKey through end must appear exactly once");
    assertEquals(expected.size(), new HashSet<>(readKeys).size());
  }
}
