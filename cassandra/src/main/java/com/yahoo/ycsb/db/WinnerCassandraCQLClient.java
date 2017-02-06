package com.yahoo.ycsb.db;


import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.commons.lang3.time.DateUtils;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * Cassandra CQL Client to use within the WINNER project.
 */
public class WinnerCassandraCQLClient extends CassandraCQLClient {
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      String[] keyparts = key.split(":");
      String metricname = keyparts[0];
      long timestamp = Long.parseLong(keyparts[1]) * 1000;
      Date bucketDate = DateUtils.truncate(new Date(timestamp), Calendar.HOUR);
      Double value = Double.parseDouble(keyparts[2]);
      ByteBuffer buf = ByteBuffer.allocate(8);
      buf.putDouble(value);
      buf.flip();

      Insert insertStmt = QueryBuilder.insertInto(table);
      insertStmt.value("metric", metricname);
      insertStmt.value("date", String.valueOf(bucketDate.getTime()/1000));
      insertStmt.value("event_time", timestamp);
      insertStmt.value("value", buf);
      insertStmt.value("attributes", StringByteIterator.getStringMap(values));

      insertStmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(insertStmt.toString());
      }
      if (trace) {
        insertStmt.enableTracing();
      }

      session.execute(insertStmt);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}

