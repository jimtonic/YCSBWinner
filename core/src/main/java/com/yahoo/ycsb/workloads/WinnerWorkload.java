package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The "WINNER Potential" benchmark scenario.
 *
 * @author Ferdinand Rewicki
 */
public class WinnerWorkload extends Workload {

  public static final String TABLENAME_PROPERTY = "table";
  public static final String TABLENAME_PROPERTY_DEFAULT = "samples";
  public static String table;

  public static final String METRICNAME_PROPERTY = "metric";
  public static final String METRICNAME_PROPERTY_DEFAULT = "sensormetric";
  public static String metric;

  public static final String TAG_COUNT_PROPERTY = "tagcount";
  public static final String TAG_COUNT_PROPERTY_DEFAULT = "10";
  int tagcount;

  private List<String> tagnames;

  public static final String TAG_LENGTH_PROPERTY = "taglength";
  public static final String TAG_LENGTH_PROPERTY_DEFAULT = "100";
  int taglength;

  /**
   * The size of the pool to choose tags from.
   * If set to 0, tags will be choosen randomly.
   */
  public static final String TAGPOOL_SIZE_PROPERTY = "tagpoolsize";
  public static final String TAGPOOL_SIZE_PROPERTY_DEFAULT = "100";
  int tagpoolsize = 100;
  private List<String> tagpool;

  public static final String TIMESTAMP_START_PROPERTY = "timestamp.start";
  public static final String TIMESTAMP_START_PROPERTY_DEFAULT = "1";
  long timestampStart;
  public static final String TIMESTAMP_INTERVAL_PROPERTY = "timestamp.interval";
  public static final String TIMESTAMP_INTERVAL_PROPERTY_DEFAULT = "1";
  int timestampInterval;
  public static final String TIMESTAMP_INTERVAL_UNIT_PROPERTY = "timestamp.unit";
  public static final String TIMESTAMP_INTERVAL_UNIT_PROPERTY_DEFAULT = "SECONDS";
  TimeUnit timestampUnit;

  public static final String VALUE_LOWER_BOUND_PROPERTY = "value.lb";
  public static final String VALUE_LOWER_BOUND_PROPERTY_DEFAULT = "-10.0";
  public static final String VALUE_UPPER_BOUND_PROPERTY = "value.ub";
  public static final String VALUE_UPPER_BOUND_PROPERTY_DEFAULT = "10.0";
  public static final String VALUE_TYPE_PROPERTY = "value.type";
  public static final String VALUE_TYPE_PROPERTY_DEFAULT = "double";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";
  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";


  private UnixEpochTimestampGenerator timestampGenerator;
  private UniformDoubleGenerator valueGenerator;
  private IncrementingPrintableStringGenerator tagvalueGenerator;
  private UniformIntegerGenerator tagchooser;

  int recordcount;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;


  private Measurements _measurements = Measurements.getMeasurements();


  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    metric = p.getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT);

    tagcount = Integer.parseInt(p.getProperty(TAG_COUNT_PROPERTY, TAG_COUNT_PROPERTY_DEFAULT));
    tagnames = new ArrayList<String>();
    for (int i = 0; i < tagcount; i++) {
      tagnames.add("tag" + i);
    }
    taglength = Integer.parseInt(p.getProperty(TAG_LENGTH_PROPERTY, TAG_LENGTH_PROPERTY_DEFAULT));
    tagpoolsize = Integer.parseInt(p.getProperty(TAGPOOL_SIZE_PROPERTY, TAGPOOL_SIZE_PROPERTY_DEFAULT));

    recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

    timestampStart = Long.parseLong(p.getProperty(TIMESTAMP_START_PROPERTY, TIMESTAMP_START_PROPERTY_DEFAULT));
    timestampInterval = Integer.parseInt(p.getProperty(TIMESTAMP_INTERVAL_PROPERTY, TIMESTAMP_INTERVAL_PROPERTY_DEFAULT));
    timestampUnit = TimeUnit.valueOf(p.getProperty(TIMESTAMP_INTERVAL_UNIT_PROPERTY, TIMESTAMP_INTERVAL_UNIT_PROPERTY_DEFAULT));
    timestampGenerator = new UnixEpochTimestampGenerator(timestampInterval, timestampUnit, timestampStart);

    tagvalueGenerator = new IncrementingPrintableStringGenerator(
        taglength,
        IncrementingPrintableStringGenerator.printableBasicAlphaASCIISet()
    );
    // generate tagpool
    tagpool = new ArrayList<>();
    for (int i = 0; i < tagpoolsize; i++) {
      tagpool.add(tagvalueGenerator.nextValue());
    }

    tagchooser = new UniformIntegerGenerator(0, tagpoolsize-1);

    valueGenerator = new UniformDoubleGenerator(
        Double.valueOf(p.getProperty(VALUE_LOWER_BOUND_PROPERTY, VALUE_LOWER_BOUND_PROPERTY_DEFAULT)),
        Double.valueOf(p.getProperty(VALUE_UPPER_BOUND_PROPERTY, VALUE_UPPER_BOUND_PROPERTY_DEFAULT))
    );

    insertionRetryLimit = Integer.parseInt(p.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

  }

  public HashMap<String, ByteIterator> buildTags() {
    HashMap<String, ByteIterator> tags = new HashMap<>();
    ByteIterator data;
    for (String tagkey : tagnames) {
      data = new StringByteIterator(tagpool.get(tagchooser.nextValue()));
      tags.put(tagkey, data);
    }

    return tags;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    long timestamp = timestampGenerator.nextValue();
    double value = valueGenerator.nextValue();
    HashMap<String, ByteIterator> tags = buildTags();
    String key = metric + ":" + timestamp + ":" + value;

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, key, tags);
      if (null != status && status.isOk()) {
        break;
      }

      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }
      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return false;
  }
}