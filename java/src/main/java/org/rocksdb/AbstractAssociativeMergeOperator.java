// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public abstract class AbstractAssociativeMergeOperator extends MergeOperator {

  public static class Context {
    private final boolean createIfMissing;

    public Context(final boolean createIfMissing) {
      this.createIfMissing = createIfMissing;
    }

    public boolean createIfMissing() {
      return createIfMissing;
    }
  }

  protected AbstractAssociativeMergeOperator(final long nativeHandle) {
    super(nativeHandle);
  }

  abstract public byte[] merge(byte[] key, byte[] oldvalue, byte[] newvalue, ReturnType rt) throws RocksDBException;

  /**
   * Deletes underlying C++ compaction pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the compaction filter are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected final native void disposeInternal(final long handle);
}
