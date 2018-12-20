package org.rocksdb;

import java.nio.ByteBuffer;

public abstract class AbstractAssociativeNioMergeOperator extends MergeOperator
{
  private native static long newOperator();
  private native boolean initOperator(long handle);

  public AbstractAssociativeNioMergeOperator()  {
    super(newOperator());
      initOperator(nativeHandle_) ;
  }

  @Override protected final native void disposeInternal(final long handle);
  abstract public Object merge(ByteBuffer key, ByteBuffer oldvalue, ByteBuffer newvalue, ReturnType rt) throws RocksDBException;
}
