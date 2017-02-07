package com.yahoo.ycsb.db;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

//import org.apache.http.client.config.RequestConfig;

/**
 * OpenTSDB Client to work with the WinnerWorkload.
 */
public class OpenTSDBClient extends DB {

  private static final int SUCCESS = 0;
  private URL urlQuery = null;
  private URL urlPut = null;
  private String host = "localhost";
  private String queryURL = "/api/query";
  private String putURL = "/api/put";
  private int port = 4242;
  private boolean filterForTags = true; // Versions above OpenTSDB 2.2 (included) can use this; Untested!
  private boolean useCount = true; // Versions above OpenTSDB 2.2 (included) have Count(), otherwise min is used
  private boolean useMs = true; // Millisecond or Second resolution
  private CloseableHttpClient client;
  private int retries = 3;

  private boolean debug = false;
  private boolean test = false;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    try {
      test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
      if (!getProperties().containsKey("host") && !test) {
        throw new DBException("No host given, abort.");
      }
      host = getProperties().getProperty("host", host);
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
      filterForTags = Boolean.parseBoolean(getProperties().getProperty("filterForTags", "true"));
      useCount = Boolean.parseBoolean(getProperties().getProperty("useCount", "true"));
      useMs = Boolean.parseBoolean(getProperties().getProperty("useMs", "true"));
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      urlQuery = new URL("http", host, port, queryURL);
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
      urlPut = new URL("http", host, port, putURL);
      if (debug) {
        System.out.println("URL: " + urlPut);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      if (!test) {
        client.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private JSONArray runQuery(URL url, String queryStr) {
    JSONArray jsonArr = new JSONArray();
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      postMethod.addHeader("accept", "application/json");
      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(postMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            postMethod.releaseConnection();
            return null;
          }
        }
      }
      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
          System.err.println("WARNING: Query returned 301, " +
              "that means 'API call has migrated or should be forwarded to another server'");
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
          BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          StringBuilder builder = new StringBuilder();
          String line;
          while ((line = bis.readLine()) != null) {
            builder.append(line);
          }
          jsonArr = new JSONArray(builder.toString());
        }
        EntityUtils.consumeQuietly(response.getEntity());
        postMethod.releaseConnection();
      }
    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return jsonArr;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String[] keyparts = key.split(":");
    String metric = keyparts[0];
    long timestamp = Long.parseLong(keyparts[1]);
    Double value = Double.valueOf(keyparts[2]);

    try {
      JSONObject query = new JSONObject();
      query.put("timestamp", timestamp);
      query.put("metric", metric);
      query.put("value", value);
      JSONObject queryTags = new JSONObject();
      for (Map.Entry entry : values.entrySet()) {
        queryTags.put(entry.getKey().toString(), entry.getValue().toString());
      }
      query.put("tags", queryTags);
      if (debug) {
        System.out.println("Input Query String: " + query.toString());
      }

      JSONArray jsonArr = runQuery(urlPut, query.toString());
      if (debug) {
        System.err.println("jsonArr: " + jsonArr);
      }
      if (jsonArr == null) {
        System.err.println("ERROR: Error in processing insert to metric: " + metric);
        return Status.ERROR;
      }
      return Status.OK;

    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.OK;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }
}
