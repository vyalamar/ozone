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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.ReadOptions;

/**
 * Managed {@link ReadOptions}.
 */
public class ManagedReadOptions extends ReadOptions {
  private final UncheckedAutoCloseable leakTracker = track(this);
  private final byte[] lowerBound;
  private final byte[] upperBound;
  private final ManagedSlice lowerBoundSlice;
  private final ManagedSlice upperBoundSlice;

  public ManagedReadOptions() {
    this.lowerBound = null;
    this.upperBound = null;
    this.lowerBoundSlice = null;
    this.upperBoundSlice = null;
  }

  public ManagedReadOptions(byte[] lowerBound, byte[] upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.lowerBoundSlice = lowerBound != null ? new ManagedSlice(lowerBound) : null;
    this.upperBoundSlice = upperBound != null ? new ManagedSlice(upperBound) : null;
    if (this.lowerBoundSlice != null) {
      setIterateLowerBound(this.lowerBoundSlice);
    }
    if (this.upperBoundSlice != null) {
      setIterateUpperBound(this.upperBoundSlice);
    }
    setFillCache(false);
  }

  public byte[] getLowerBound() {
    return lowerBound;
  }

  public byte[] getUpperBound() {
    return upperBound;
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      try {
        if (lowerBoundSlice != null) {
          lowerBoundSlice.close();
        }
      } finally {
        try {
          if (upperBoundSlice != null) {
            upperBoundSlice.close();
          }
        } finally {
          leakTracker.close();
        }
      }
    }
  }
}
