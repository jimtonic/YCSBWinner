package com.yahoo.ycsb.db;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import org.apache.commons.lang.time.DateUtils;
import org.bson.Document;
import org.joda.time.DateTime;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static java.lang.Double.NaN;


/**
 * MongoDB Client to use within the WINNER project.
 */
public class WinnerMongoDbClient extends MongoDbClient {

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {

    MongoCollection<Document> collection = database.getCollection(table);

    String[] keyparts = key.split(":");
    String metricname = keyparts[0];
    long timestamp = Long.parseLong(keyparts[1]);
    Double value = Double.parseDouble(keyparts[2]);
    DateTime date = new DateTime(timestamp*1000);
    Date bucketDate = DateUtils.truncate(date.toDate(), Calendar.HOUR);
    String rowKey = metricname+":"+(bucketDate.getTime()/1000);

    int minute = date.getMinuteOfHour();
    int second = date.getSecondOfMinute();

    Document update = new Document();
    Document tags = new Document();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      tags.append(entry.getKey(), entry.getValue().toString());
    }
    update.append("$set", new Document("data."+minute+"."+second, value));
    update.append("$setOnInsert", tags);

    try {
      UpdateResult result = collection.updateOne(
          eq("_id", rowKey),
          update,
          UPDATE_WITH_UPSERT
      );
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Generate a pre-initalized document with a 59*59 data array.
   *
   * @param value the value to insert
   * @param minute the minute to insert the value
   * @param second the second to insert the value
   * @return The generated document.
   */
  private Document generateDataDocument(Double value, int minute, int second) {
    Document doc = new Document();

    for (int i = 0; i < 60; i++) {
      Document subdoc = new Document();
      for (int j = 0; j < 60; j++) {
        if (i == minute && j == second) {
          subdoc.append(String.valueOf(j), value);
        } else {
          subdoc.append(String.valueOf(j), NaN);
        }
      }
      doc.append(String.valueOf(i), subdoc);
    }

    return doc;
  }
}
