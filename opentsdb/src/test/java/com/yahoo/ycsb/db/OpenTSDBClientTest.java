package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Properties;

public class OpenTSDBClientTest {

  private OpenTSDBClient client;
  private UnixEpochTimestampGenerator tGen;
  private UniformDoubleGenerator vGen;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("host", "localhost");
    p.setProperty("port", "4242");

    Measurements.setProperties(p);
    final WinnerWorkload workload = new WinnerWorkload();
    workload.init(p);

    client = new OpenTSDBClient();
    client.setProperties(p);
    client.init();

    tGen = new UnixEpochTimestampGenerator(1, java.util.concurrent.TimeUnit.SECONDS, 1);
    vGen = new UniformDoubleGenerator(-10.0, 10.0);
  }

  @Test
  public void testInsert() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Status status = client.insert("samples", getNextKey(), getTags());
      Assert.assertEquals(Status.OK, status);
    }
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
