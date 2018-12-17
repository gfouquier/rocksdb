// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public abstract class AbstractAssociativeMergeOperatorFactory<T extends AbstractAssociativeMergeOperator>
  extends RocksCallbackObject {

  public AbstractAssociativeMergeOperatorFactory() {
    super(null);
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewAssociativeMergeOperator();
  }

  private long createAssociativeMergeOperator(boolean createIfMissing) {
    final T mergeOperator = createAssociativeMergeOperator(
      new AbstractAssociativeMergeOperator.Context(createIfMissing)
    );
    mergeOperator.disOwnNativeHandle();
    return mergeOperator.nativeHandle_;
  }

  public abstract T createAssociativeMergeOperator(final AbstractAssociativeMergeOperator.Context context);

  public abstract String name();

  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native long createNewAssociativeMergeOperator();
  private native void disposeInternal(final long handle);
}
