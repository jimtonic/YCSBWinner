package com.yahoo.ycsb.db;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class WinnerMongoDbClientTest {

  private UnixEpochTimestampGenerator tGen;
  private UniformDoubleGenerator vGen;
  private WinnerWorkload workload;
  private WinnerMongoDbClient client;

  @Before
  public void setUp() throws Exception {
    tGen = new UnixEpochTimestampGenerator(1, TimeUnit.SECONDS, 0);
    vGen = new UniformDoubleGenerator(-10.0, 10.0);
    client = new WinnerMongoDbClient();
    client.init();
  }

  @Test
  public void testInsert() throws Exception {

    for (int i = 0; i < 10000; i++) {
      Status status = client.insert("samples", getNextKey(), getTags());
      assertEquals(Status.OK, status);
    }
  }

  @After
  public void tearDown() throws Exception {
    client.cleanup();

  }

  private String getNextKey() {
    long ts = tGen.nextValue();
    double val = vGen.nextValue();
    String key = "sensormetric:"+ts+":"+val;
    return key;
  }

  private HashMap<String, ByteIterator> getTags() {
    HashMap<String, String> tags = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      tags.put("tag"+i, "value"+i);
    }
    return StringByteIterator.getByteIteratorMap(tags);
  }
}
