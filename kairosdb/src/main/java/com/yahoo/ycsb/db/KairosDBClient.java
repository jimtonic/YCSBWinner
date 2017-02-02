package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * KairosDB Client.
 */
public class KairosDBClient extends DB {

  public static final String HOST_PROPERTY = "host";
  public static final String HOST_PROPERTY_DEFAULT = "localhost";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "8071";

  private HttpClient client = null;
  private String host = "localhost";
  private String port;
  public static final String METRICNAME_PROPERTY = "metric";
  public static final String METRICNAME_PROPERTY_DEFAULT = "sensormetric";
  private static String metric;


  private boolean debug = false;
  private boolean test = false;

  @Override
  public void init() throws DBException {
    try {
      test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
      host = getProperties().getProperty(HOST_PROPERTY);
      if (host == null && !test) {
        throw new DBException(String.format(
            "Required property \"%s\" missing for KairosDBClient",
            HOST_PROPERTY));
      }
      port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
      metric = getProperties().getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT);
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      if (!test) {
        client = new HttpClient(String.format("http://%s:%s", host, port));
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
  }

  public HashMap<String, String> generateTagpool(int size, int taglength) {
    HashMap<String, String> tp = new HashMap<String, String>();
    IncrementingPrintableStringGenerator generator = new IncrementingPrintableStringGenerator(
        taglength,
        IncrementingPrintableStringGenerator.printableBasicAlphaNumericASCIISet()
    );

    for (int i = 0; i < size; i++) {
      tp.put("tag".concat(String.valueOf(i)), generator.nextValue());
    }

    return tp;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (!test) {
        client.deleteMetric(metric);
        client.shutdown();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result
  ) {
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }


  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {

    HashMap<String, String> tags = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      tags.put(entry.getKey(), entry.getValue().toString());
    }

    MetricBuilder builder = MetricBuilder.getInstance();
    String[] keyparts = key.split(":");
    Metric met = builder.addMetric(keyparts[0]);
    met.addDataPoint(Long.parseLong(keyparts[1]), Double.parseDouble(keyparts[2]))
        .addTags(tags);

    try {
      Response response = client.pushMetrics(builder);
      if (response.getStatusCode() == 204) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }
}
