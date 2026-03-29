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

import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueSpliterator;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * A Spliterator for RocksDB tables that partitions the keyspace using SST file boundaries.
 */
class ByteArrayRawSpliterator implements KeyValueSpliterator<byte[], byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(ByteArrayRawSpliterator.class);

  private final RDBTable table;
  private final List<byte[]> boundaries;
  private final boolean closeOnEx;
  private final int maxParallelism;
  private final int splitDepth;

  private Table.KeyValueIterator<byte[], byte[]> currentIterator;
  private boolean hasAdvanced = false;
  
  // Custom comparator for unsigned byte array comparison
  private static final Comparator<byte[]> UNSIGNED_BYTES_COMPARATOR = (a, b) -> {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    int minLen = Math.min(a.length, b.length);
    for (int i = 0; i < minLen; i++) {
        int aVal = a[i] & 0xFF;
        int bVal = b[i] & 0xFF;
        if (aVal != bVal) {
            return aVal - bVal;
        }
    }
    return a.length - b.length;
  };

  /**
   * Root constructor that extracts bounds from SST files.
   * Boundaries represent the keyspace partition: [lowerBound, upperBound]
   * The list is always sorted and includes the effective lower bound and all SST split points.
   *
   * @param table the RDBTable to iterate over
   * @param startKey optional starting key (acts as effective lower bound)
   * @param sstFiles SST file metadata to extract boundary points
   * @param closeOnEx reserved for future use; currently close() always cleans up resources
   * @param maxParallelism maximum split depth; stops splitting when depth >= maxParallelism
   *                       (creates at most 2^maxParallelism leaf spliterators in worst case)
   */
  ByteArrayRawSpliterator(RDBTable table, byte[] startKey,
                          List<LiveFileMetaData> sstFiles, boolean closeOnEx, int maxParallelism) {
    this.table = table;
    this.closeOnEx = closeOnEx;
    this.maxParallelism = maxParallelism;
    this.splitDepth = 0;

    TreeSet<byte[]> uniqueBounds = new TreeSet<>(UNSIGNED_BYTES_COMPARATOR);
    for (LiveFileMetaData file : sstFiles) {
      if (file.smallestKey() != null) {
        uniqueBounds.add(file.smallestKey());
      }
      if (file.largestKey() != null) {
        uniqueBounds.add(file.largestKey());
      }
    }

    // Build boundaries: always starts with lowerBound (startKey or null),
    // then sorted SST split points (excluding duplicates of startKey), ends with null (unbounded upper)
    this.boundaries = new ArrayList<>();
    this.boundaries.add(startKey); // Lower bound: null means absolute beginning

    // Add only SST boundaries that are > startKey (skip duplicates when bound == startKey)
    for (byte[] bound : uniqueBounds) {
      if (startKey == null) {
        this.boundaries.add(bound);
      } else {
        int cmp = UNSIGNED_BYTES_COMPARATOR.compare(bound, startKey);
        // Only add if bound > startKey; skip if bound == startKey (avoid duplicate)
        if (cmp > 0) {
          this.boundaries.add(bound);
        }
      }
    }

    this.boundaries.add(null); // Upper bound: null means absolute end of keyspace
  }

  /**
   * Internal constructor for trySplit.
   */
  private ByteArrayRawSpliterator(RDBTable table, List<byte[]> boundaries, boolean closeOnEx,
                                  int maxParallelism, int splitDepth) {
    this.table = table;
    this.boundaries = boundaries;
    this.closeOnEx = closeOnEx;
    this.maxParallelism = maxParallelism;
    this.splitDepth = splitDepth;
  }

  private void initIteratorIfNeeded() {
    if (currentIterator == null && !boundaries.isEmpty()) {
      try {
        byte[] lowerBound = boundaries.get(0);
        byte[] upperBound = boundaries.get(boundaries.size() - 1);
        
        // Pass the explicit bounds down to the native RocksDB options
        currentIterator = table.iterator(lowerBound, upperBound, IteratorType.KEY_AND_VALUE);
        
        if (lowerBound != null) {
          currentIterator.seek(lowerBound);
        } else {
          currentIterator.seekToFirst();
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize iterator", e);
      }
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super KeyValue<byte[], byte[]>> action) {
    initIteratorIfNeeded();
    hasAdvanced = true;

    if (currentIterator != null && currentIterator.hasNext()) {
      KeyValue<byte[], byte[]> next = currentIterator.next();
      // Native RocksDB bounds enforcement via iterate_upper_bound handles bound checks;
      // trust native implementation instead of redundant Java-side checks
      action.accept(next);
      return true;
    }
    return false;
  }

  @Override
  public Spliterator<KeyValue<byte[], byte[]>> trySplit() {
    // Cannot split after iteration has started (would cause stale bounds)
    if (hasAdvanced) {
      return null;
    }

    // Limit parallelism by depth: each recursive split increments depth.
    // Stop splitting when depth >= maxParallelism (max split depth, not leaf count).
    // E.g., maxParallelism=4 allows splits at depth 0,1,2,3 (4 levels), creating up to 2^4=16 leaf spliterators.
    if (splitDepth >= maxParallelism) {
      return null;
    }

    // If we have fewer than 2 boundaries, we cannot split further
    if (boundaries.size() <= 2) {
      return null;
    }

    // Close any active iterator before splitting to reset state
    if (currentIterator != null) {
      try {
        currentIterator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close iterator during split", e);
      }
      currentIterator = null;
    }

    int mid = boundaries.size() / 2;
    List<byte[]> leftBoundaries = new ArrayList<>(boundaries.subList(0, mid + 1));
    List<byte[]> rightBoundaries = new ArrayList<>(boundaries.subList(mid, boundaries.size()));

    // Keep right bounds for ourselves, give left bounds to the new Spliterator
    this.boundaries.clear();
    this.boundaries.addAll(rightBoundaries);

    return new ByteArrayRawSpliterator(table, leftBoundaries, closeOnEx, maxParallelism, splitDepth + 1);
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE; // We don't know the exact number of keys
  }

  @Override
  public int characteristics() {
    return Spliterator.ORDERED | Spliterator.NONNULL;
  }

  @Override
  public void close() throws Exception {
    if (currentIterator != null) {
      // Always cleanup native resources unconditionally
      // closeOnEx should only control exception handling behavior, not whether cleanup happens
      currentIterator.close();
      currentIterator = null;
    }
  }
}
