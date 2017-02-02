package com.yahoo.ycsb.db;

import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;

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
  public void testInsertStaticDatapoint() throws Exception {
    final HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("tag0", "value1");
    tags.put("tag1", "value2");

    Long currentTimestamp = System.currentTimeMillis() - new Long("2678400000");
    Double value = 100.0;

    String key = METRIC + ":" + currentTimestamp + ":" + value;

    Status status = client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(tags));
    assertEquals(Status.OK, status);

    java.util.concurrent.TimeUnit.SECONDS.sleep(1);

    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(2, TimeUnit.MONTHS)
        .addMetric(METRIC)
        .addAggregator(AggregatorFactory.createAverageAggregator(10, TimeUnit.MONTHS));

    QueryResponse response = httpClient.query(builder);

    assertEquals(response.getStatusCode(), 200);
    assertThat(response.getBody(), containsString("\"sample_size\":1"));
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
}
