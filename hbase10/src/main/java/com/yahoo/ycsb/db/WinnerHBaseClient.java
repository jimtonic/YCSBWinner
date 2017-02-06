package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * HBase Client to use within the WINNER project.
 *
 * Create Table 'samples' and columnfamily 'cf': create 'samples', 'cf'
 */
public class WinnerHBaseClient extends HBaseClient10 {

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

}
