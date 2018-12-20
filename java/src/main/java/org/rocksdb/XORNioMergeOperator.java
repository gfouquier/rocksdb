package org.rocksdb;


public class XORNioMergeOperator extends MergeOperator
{
  private native static long newOperator();
  private native boolean initOperator(long handle);

  public XORNioMergeOperator()  {
    super(newOperator());
      initOperator(nativeHandle_) ;
  }

  @Override protected final native void disposeInternal(final long handle);
}
