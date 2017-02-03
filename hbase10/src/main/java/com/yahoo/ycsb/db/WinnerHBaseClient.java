package com.yahoo.ycsb.db;

import com.google.common.annotations.VisibleForTesting;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * HBase Client to use within the WINNER project.
 */
public class WinnerHBaseClient extends HBaseClient10 {
  private Configuration config = HBaseConfiguration.create();

  private static AtomicInteger threadCount = new AtomicInteger(0);

  private static Connection connection = null;
  private static final Object CONNECTION_LOCK = new Object();
  private String tableName = "";
  private Table currentTable = null;

  private String columnFamily = "";
  private byte[] columnFamilyBytes;


  @Override
  public void init() throws DBException {
//    tableName = TableName.valueOf(getProperties().getProperty("table"));
//    config.set("hbase.zookeeper.property.clientPort", "2181");
//    config.set("hbase.zookeeper.quorum", "localhost");

    try {
      threadCount.getAndIncrement();
      synchronized (CONNECTION_LOCK) {
        if (connection == null) {
          // Initialize if not set up already.
          connection = ConnectionFactory.createConnection(config);
        }
      }
    } catch (java.io.IOException e) {
      throw new DBException(e);
    }

    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);

    String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    try {
      final TableName tName = TableName.valueOf(table);
      synchronized (CONNECTION_LOCK) {
        connection.getTable(tName).getTableDescriptor();
      }
    } catch (IOException e) {
      System.out.println("---- TABLE SOES NOT EXIST! ----");
      throw new DBException(e);
    }

  }

  @Override
  public void cleanup() throws DBException {

  }

  public void getHTable(String table) throws IOException {
    final TableName tName = TableName.valueOf(table);
    synchronized (CONNECTION_LOCK) {
      this.currentTable = connection.getTable(tName);
    }
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
    String metricname = keyparts[0];
    long timestamp = Long.parseLong(keyparts[1]);
    Double value = Double.parseDouble(keyparts[2]);

    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        getHTable(table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    String rowkey = metricname + ":" + timestamp;
    Put p = new Put(Bytes.toBytes(rowkey));
    p.addColumn(columnFamilyBytes, Bytes.toBytes("metric"), timestamp, Bytes.toBytes(metricname));
    p.addColumn(columnFamilyBytes, Bytes.toBytes("value"), timestamp, Bytes.toBytes(value));
    for (Map.Entry<String, ByteIterator> tag : values.entrySet()) {
      byte[] tval = tag.getValue().toArray();
      p.addColumn(columnFamilyBytes, Bytes.toBytes(tag.getKey()), timestamp, tval);
    }

    try {
      currentTable.put(p);
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;

  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

  @VisibleForTesting
  void setConfiguration(final Configuration newConfig) {
    this.config = newConfig;
  }

  @VisibleForTesting
  Connection getConnection() {
    return connection;
  }
}
