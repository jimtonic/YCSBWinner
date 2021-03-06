package com.yahoo.ycsb.db;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestWinnerCassandraCQLClient {
  // Change the default Cassandra timeout from 10s to 120s for slow CI machines
  private final static long timeout = 120000L;

  private final static String TABLE = "samples";
  private final static String METRIC = "sensormetric";
  private final static String HOST = "localhost";
  private final static String PORT = "9042";
  private final static int TAGPOOL_SIZE = 5;
  private final static int TAGCOUNT = 5;
  private final static int TAGLENGTH = 10;

  private WinnerCassandraCQLClient client;
  private Session session;

  private List<String> tagpool;

  UnixEpochTimestampGenerator tGen;
  UniformDoubleGenerator vGen;
  IncrementingPrintableStringGenerator tagGen;


  @ClassRule
  public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(
      new ClassPathCQLDataSet("samples.cql", "ycsb"), null, timeout);

  @Before
  public void setUp() throws Exception {
    session = cassandraUnit.getSession();
    Properties p = new Properties();
    p.setProperty("hosts", HOST);
    p.setProperty("port", PORT);
    p.setProperty("table", TABLE);
    p.setProperty("metric", METRIC);
    p.setProperty("fieldlength", String.valueOf(TAGLENGTH));
    p.setProperty("fieldcount", String.valueOf(TAGCOUNT));
    Measurements.setProperties(p);

    final WinnerWorkload workload = new WinnerWorkload();
    workload.init(p);
    client = new WinnerCassandraCQLClient();
    client.setProperties(p);
    client.init();

    tGen = new UnixEpochTimestampGenerator(1, TimeUnit.SECONDS, 1);
    vGen = new UniformDoubleGenerator(-10.0, 10.0);
    tagGen = new IncrementingPrintableStringGenerator(
        TAGLENGTH, IncrementingPrintableStringGenerator.printableBasicAlphaASCIISet());

    tagpool = new ArrayList<>();
    for (int i = 0; i < TAGPOOL_SIZE; i++) {
      tagpool.add(tagGen.nextValue());
    }
  }

  @After
  public void tearDownClient() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    client = null;
  }

  @After
  public void clearTable() throws Exception {
    // Clear the table so that each test starts fresh.
    final Statement truncate = QueryBuilder.truncate(TABLE);
    if (cassandraUnit != null) {
      cassandraUnit.getSession().execute(truncate);
    }
  }

  @Test
  public void testInsert() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Status status = client.insert("samples", getNextKey(), generateTags());
      Assert.assertEquals(Status.OK, status);
    }
  }

  private String getNextKey() {
    long ts = tGen.nextValue();
    double val = vGen.nextValue();
    String key = "sensormetric:"+ts+":"+val;
    return key;
  }

  private HashMap<String, ByteIterator> generateTags() {
    HashMap<String, ByteIterator> tags = new HashMap<>();
    int i = 0;
    for (String tag : tagpool) {
      tags.put("tag"+i, new StringByteIterator(tag));
      i++;
    }
    return tags;
  }
}
