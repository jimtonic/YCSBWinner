package com.yahoo.ycsb.workloads;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestWinnerWorkload {

  private static int tagcount = 10;
  private static int taglength = 100;

  private WinnerWorkload workload;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("tagcount", String.valueOf(tagcount));
    p.setProperty("taglength", String.valueOf(taglength));

    Measurements.setProperties(p);
    workload = new WinnerWorkload();
    workload.init(p);
  }

  @Test
  public void testBuildTags() throws Exception {
    HashMap<String, ByteIterator> tags = workload.buildTags();

    assertEquals(tagcount, tags.size());
    for (HashMap.Entry<String, ByteIterator> entry : tags.entrySet()) {
      assertEquals(taglength, entry.getValue().toString().length());
    }
  }

  @Test
  public void testKeysplit() throws Exception {
    UniformDoubleGenerator gen = new UniformDoubleGenerator(-100.0, 100.0);
    String key = "samples:" + System.currentTimeMillis() + ":" + gen.nextValue();
    System.out.println("key: " + key);

    System.out.println("-------");
    String[] keyparts = key.split(":");
    System.out.println("metric:");
    System.out.println(keyparts[0]);
    System.out.println(keyparts[0]);

    System.out.println("-------");
    System.out.println("timestamp:");
    System.out.println(keyparts[1]);
    System.out.println(Long.parseLong(keyparts[1]));

    System.out.println("-------");
    System.out.println("value:");
    System.out.println(keyparts[2]);
    System.out.println(Double.parseDouble(keyparts[2]));


  }
}
