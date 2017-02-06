package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.junit.*;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class TestKairosDBClient {

  private final static String TABLE = "samples";
  private final static String METRIC = "sensormetric";
  private final static String HOST = "localhost";
  private final static String PORT = "8071";

  private KairosDBClient client;
  private HttpClient httpClient;

  private UnixEpochTimestampGenerator tGen;
  private UniformDoubleGenerator vGen;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("host", HOST);
    p.setProperty("metric", METRIC);
    p.setProperty("port", PORT);
    p.setProperty("table", TABLE);
    p.setProperty("fieldlength", "10");
    p.setProperty("fieldcount", "5");

    Measurements.setProperties(p);
    final WinnerWorkload workload = new WinnerWorkload();
    workload.init(p);
    client = new KairosDBClient();
    client.setProperties(p);
    client.init();

    httpClient = new HttpClient(String.format("http://%s:%s", HOST, PORT));

    tGen = new UnixEpochTimestampGenerator(1, java.util.concurrent.TimeUnit.SECONDS, 1);
    vGen = new UniformDoubleGenerator(-10.0, 10.0);
  }

  @After
  public void tearDownClient() throws Exception {
    if (client != null) {
      httpClient.deleteMetric(METRIC);
      client.cleanup();
    }
    client = null;
  }

  @Test
  @Ignore
  public void testInsert() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Status status = client.insert("samples", getNextKey(), getTags());
      Assert.assertEquals(Status.OK, status);
    }

    java.util.concurrent.TimeUnit.SECONDS.sleep(1);

    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(1))
        .addMetric(METRIC)
        .addAggregator(AggregatorFactory.createCountAggregator(100, TimeUnit.YEARS));
    QueryResponse response = httpClient.query(builder);
    assertEquals(response.getStatusCode(), 200);
    assertThat(response.getBody(), containsString("\"sample_size\":1000"));
  }

  @Test
  @Ignore
  public void testGenerateTagpool() throws Exception {
    HashMap<String,String> tagpool = client.generateTagpool(10, 10);

    assertEquals(10, tagpool.size());
    for (Map.Entry<String,String> entry : tagpool.entrySet()) {
      assertEquals(10, entry.getValue().length());
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
