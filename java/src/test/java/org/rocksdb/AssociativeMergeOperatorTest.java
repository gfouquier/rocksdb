package org.rocksdb;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class AssociativeMergeOperatorTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  class ConcatAssociativeMergeOperator extends AbstractAssociativeMergeOperator {

    public ConcatAssociativeMergeOperator() { super(0l); }

		@Override
		public byte[] merge(byte[] key, byte[] oldvalue, byte[] newvalue, ReturnType rt) {
		  if (oldvalue == null) {
		    rt.isArgumentReference = true;
		    return newvalue;
		  }
		  StringBuffer sb = new StringBuffer(new String(oldvalue));
		  sb.append(',');
		  sb.append(new String(newvalue));
		  System.out.println("----execute merge---" + new String(newvalue));
		  return sb.toString().getBytes();
		}
  }

  private class ConcatAssociativeMergeOperatorFactory extends AbstractAssociativeMergeOperatorFactory<ConcatAssociativeMergeOperator> {
    @Override
    public ConcatAssociativeMergeOperator createAssociativeMergeOperator(final AbstractAssociativeMergeOperator.Context context) {
      return new ConcatAssociativeMergeOperator();
    }

    @Override
    public String name() {
      return "ConcatAssociativeMergeOperator";
    }
  }

  @Test
  public void mergeWithConcatAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("---- mergeWithConcatAssociativeOperator ---");

          try (final ConcatAssociativeMergeOperatorFactory factory = new ConcatAssociativeMergeOperatorFactory();
               final ConcatAssociativeMergeOperator stringAppendOperator = factory.createAssociativeMergeOperator(new AbstractAssociativeMergeOperator.Context(true));
               final Options opt = new Options()
                 .setCreateIfMissing(true)
                 .setMergeOperator(stringAppendOperator);
               final WriteOptions wOpt = new WriteOptions();
               final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())
          ) {
            db.merge("key1".getBytes(), "value".getBytes());
            assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());

            // merge key1 with another value portion
            System.out.println("->merge value2");
            db.merge("key1".getBytes(), "value2".getBytes());
            System.out.println("->get");
            assertThat(db.get("key1".getBytes())).isEqualTo("value,value2".getBytes());
            System.out.println("->merge value3");

            // merge key1 with another value portion
            db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
            System.out.println("->get");
            assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3".getBytes());
            System.out.println("->merge value4");
            db.merge(wOpt, "key1".getBytes(), "value4".getBytes());
            System.out.println("->get");
            assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3,value4".getBytes());

            // merge on non existent key shall insert the value
            db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
            assertThat(db.get("key2".getBytes())).isEqualTo("xxxx".getBytes());
          }
        } catch (Exception e){
          throw new RuntimeException(e);
        } finally {
        }
      }
    });
    t.setDaemon(false);
    t.start();
    t.join();
  }
}