// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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

public class AbstractMergeOperatorTest {
	
  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

	private class AbstractAssociativeMergeOperatorTest extends AbstractAssociativeMergeOperator {

		public AbstractAssociativeMergeOperatorTest() throws RocksDBException {
		  super();
			System.out.println("---- AbstractAssociativeMergeOperatorTest -> super ---");
		}

		@Override
		public byte[] merge(byte[] key, byte[] oldvalue, byte[] newvalue, ReturnType rt) {
		  if (oldvalue == null) {
		    rt.isArgumentReference=true;
		    return newvalue;
		  }
		  StringBuffer sb = new StringBuffer(new String(oldvalue));
		  sb.append(',');
		  sb.append(new String(newvalue));
		  System.out.println("----execute merge---" + new String(newvalue));
		  return sb.toString().getBytes();
		}
	}

	@Test
	public void mergeWithAbstractAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
		Thread t = new Thread(new Runnable() {
		  @Override
		  public void run() {
		    try {
						System.out.println("---- mergeWithAbstractAssociativeOperator ---");
		        try (final AbstractAssociativeMergeOperatorTest stringAppendOperator = new AbstractAssociativeMergeOperatorTest();
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

	private class AbstractNotAssociativeMergeOperatorTest extends AbstractNotAssociativeMergeOperator {

		public AbstractNotAssociativeMergeOperatorTest() throws RocksDBException {
		  super(true, false, false);
		}

		private byte[] collect(byte[][] operands) {
		  StringBuffer sb = new StringBuffer();
		  for(int i = 0; i < operands.length; i++) {
		    if (i > 0) 
					sb.append(',');
		    sb.append(new String(operands[i]));
		  }
		  return sb.toString().getBytes();
		}

		@Override
		public byte[] fullMerge(byte[] key, byte[] oldvalue, byte[][] operands, ReturnType rt) throws RocksDBException {
		  System.out.println("execute fullMerge" + oldvalue);
		  if (oldvalue == null) 
		    return collect(operands);
		  return (new String(oldvalue) + ',' + new String(collect(operands))).getBytes();
		}

		@Override
		public byte[] partialMultiMerge(byte[] key,  byte[][] operands, ReturnType rt) {
		  System.out.println("execute partialMultiMerge");
		  return (new String(collect(operands))).getBytes();
		}

		@Override
		public byte[] partialMerge(byte[] key, byte[] left, byte[] right, ReturnType rt) {
		  System.out.println("execute partialMerge");
		  StringBuffer sb = new StringBuffer(new String(left));
		  sb.append(',');
		  sb.append(new String(right));

		  return sb.toString().getBytes();
		}

		@Override
		public boolean shouldMerge(byte[][] operands) {
		  System.out.println("execute shouldMerge");
		  return true;
		}
	}

	@Test
	public void mergeWithAbstractNotAssociativeOperator() throws RocksDBException, NoSuchMethodException, InterruptedException {
		/*Thread t = new Thread(new Runnable() {
		  @Override
		  public void run() {*/
		      try {
		        try (final AbstractNotAssociativeMergeOperatorTest stringAppendOperator = new AbstractNotAssociativeMergeOperatorTest();
		             final Options opt = new Options()
		                     .setCreateIfMissing(true)
		                     .setMergeOperator(stringAppendOperator);
		             final WriteOptions wOpt = new WriteOptions();
		             final RocksDB db = RocksDB.open(opt, dbFolder.getRoot().getAbsolutePath())
		        ) {
		          db.put("key1".getBytes(), "value".getBytes());
		          assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());

		          // merge key1 with another value portion
		          db.merge("key1".getBytes(), "value2".getBytes());
		          System.out.println(new String(db.get("key1".getBytes())));
		          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2".getBytes());

		          // merge key1 with another value portion
		          db.merge(wOpt, "key1".getBytes(), "value3".getBytes());
		          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3".getBytes());
		          db.merge(wOpt, "key1".getBytes(), "value4".getBytes());
		          System.out.println(new String(db.get("key1".getBytes())));
		          assertThat(db.get("key1".getBytes())).isEqualTo("value,value2,value3,value4".getBytes());

		          // merge on non existent key shall insert the value
		          db.merge(wOpt, "key2".getBytes(), "xxxx".getBytes());
		          assertThat(db.get("key2".getBytes())).isEqualTo("xxxx".getBytes());
		        }
		      } catch (Exception e){
		        throw new RuntimeException(e);
		      } finally {
		      }
		/*    }
		});
		t.setDaemon(false);
		t.start();
		t.join();*/
	}
}

