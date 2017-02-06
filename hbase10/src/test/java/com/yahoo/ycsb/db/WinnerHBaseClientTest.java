package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.UniformDoubleGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.WinnerWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assume.assumeTrue;

public class WinnerHBaseClientTest {

  private final static String TABLE = "samples";
  private final static String COLUMN_FAMILY = "cf";

  private static HBaseTestingUtility testingUtil;
  private WinnerHBaseClient client;
  private Table table = null;

  private UnixEpochTimestampGenerator tGen;
  private UniformDoubleGenerator vGen;

  private static boolean isWindows() {
    final String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  /**
   * Creates a mini-cluster for use in these tests.
   *
   * This is a heavy-weight operation, so invoked only once for the test class.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // Minicluster setup fails on Windows with an UnsatisfiedLinkError.
    // Skip if windows.
    assumeTrue(!isWindows());
    testingUtil = HBaseTestingUtility.createLocalHTU();
    testingUtil.startMiniCluster();
  }

  /**
   * Tears down mini-cluster.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    if (testingUtil != null) {
      testingUtil.shutdownMiniCluster();
    }
  }

  @Before
  public void setUp() throws Exception {
    client = new WinnerHBaseClient();
    client.setConfiguration(new Configuration(testingUtil.getConfiguration()));

    Properties p = new Properties();
    p.setProperty("table", TABLE);
    p.setProperty("columnfamily", COLUMN_FAMILY);

    table = testingUtil.createTable(TableName.valueOf(TABLE), Bytes.toBytes(COLUMN_FAMILY));

    Measurements.setProperties(p);
    final WinnerWorkload workload = new WinnerWorkload();
    workload.init(p);

    client.setProperties(p);
    client.init();

    tGen = new UnixEpochTimestampGenerator(1, java.util.concurrent.TimeUnit.SECONDS, 1);
    vGen = new UniformDoubleGenerator(-10.0, 10.0);
  }

  @After
  public void tearDown() throws Exception {
    table.close();
    testingUtil.deleteTable(TABLE);
  }

  @Test
  public void testInsert() throws Exception {
    for (int i = 0; i < 1000; i++) {
      Status status = client.insert("samples", getNextKey(), getTags());
      Assert.assertEquals(Status.OK, status);
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
