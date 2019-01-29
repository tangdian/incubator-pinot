package org.apache.pinot.thirdeye.datasource.presto;

import com.google.common.cache.CacheLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeDataFrameResultSet.*;



public class PrestoResponseCacheLoader extends CacheLoader<PrestoQuery, ThirdEyeResultSet> {
  private static final Logger LOG = LoggerFactory.getLogger(PrestoResponseCacheLoader.class);

  private static final long CONNECTION_TIMEOUT = 60000;
  private static final int INIT_CONNECTIONS = 10;

  private static int MAX_CONNECTIONS;
  static {
    try {
      MAX_CONNECTIONS = Integer.parseInt(System.getProperty("max_presto_connections", "25"));
    } catch (Exception e) {
      MAX_CONNECTIONS = 25;
    }
  }
  private final String PRESTO_URL = "prestoURLs";
  private static final String JDBC_USER = "user";
  private static final String JDBC_PASSWORD = "password";

  Map<String, DataSource> dbNameToDataSourceMap = new HashMap<>();
  private String user;
  private String password;


  public PrestoResponseCacheLoader(Map<String, Object> properties) throws Exception {
    Map<String, String> dbNameToURLMap = ConfigUtils.getMap(properties.get(PRESTO_URL));
    user = (String)properties.get(JDBC_USER);
    password = (String)properties.get(JDBC_PASSWORD);

    for (Map.Entry<String, String> entry: dbNameToURLMap.entrySet()) {
      DataSource dataSource = new DataSource();
//      dataSource.setDriverClassName("com.facebook.presto");
      dataSource.setInitialSize(INIT_CONNECTIONS);
      dataSource.setMaxActive(MAX_CONNECTIONS);
      dataSource.setUsername(user);
      dataSource.setPassword(password);
      dataSource.setUrl(entry.getValue());

      dataSource.setValidationQuery("select 1");
      dataSource.setTestWhileIdle(true);
      dataSource.setTestOnBorrow(true);
      // when returning connection to pool
      dataSource.setTestOnReturn(true);
      dataSource.setRollbackOnReturn(true);

      // Timeout before an abandoned(in use) connection can be removed.
      dataSource.setRemoveAbandonedTimeout(600_000);
      dataSource.setRemoveAbandoned(true);

      dbNameToDataSourceMap.put(entry.getKey(), dataSource);
    }
  }

  public Map<String, List<String>> getDimensionFilters(String dataset) {
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
    DateTimeZone timeZone = Utils.getDataTimeZone(dataset);
    long maxTime = 0;

    String[] tableComponents = dataset.split("\\.");
    String dbName = tableComponents[0];
    String tableName = ThirdEyeUtils.computePrestoTableName(dataset);
    Map<String, List<String>> dimensionFilters = new HashMap<>();
    Connection conn = null;
    DataSource dataSource = dbNameToDataSourceMap.get(dbName);

    try {
      for (String dimension: datasetConfig.getDimensions()) {
        dimensionFilters.put(dimension, new ArrayList<>());
        String sqlQuery = "SELECT DISTINCT(" + dimension + ") FROM " + tableName;
        conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQuery);
        while (rs.next()) {
          dimensionFilters.get(dimension).add(rs.getString(1));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return dimensionFilters;
  }

  public long getMaxDataTime(String dataset) throws Exception {
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
    DateTimeZone timeZone = Utils.getDataTimeZone(dataset);
    long maxTime = 0;

    String[] tableComponents = dataset.split("\\.");
    String dbName = tableComponents[0];
    String tableName = ThirdEyeUtils.computePrestoTableName(dataset);
    String sqlQuery = "SELECT MAX(" + timeSpec.getColumnName() + ") FROM " + tableName + " LIMIT 1000";
    Connection conn = null;
    DataSource dataSource = dbNameToDataSourceMap.get(dbName);

    try {
      conn = dataSource.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sqlQuery);
      if (rs.next()) {
        String maxTimeString = rs.getString(1);
        String timeFormat = timeSpec.getFormat();

        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(Long.valueOf(maxTimeString) - 1, timeZone);
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(timeZone);
          DateTime endDateTime = DateTime.parse(maxTimeString, inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
      throw new RuntimeException(e);
    } finally {
      conn.close();
    }
    return maxTime;
  }


  @Override
  public ThirdEyeResultSet load(PrestoQuery prestoQuery) throws Exception {
    DataSource dataSource = dbNameToDataSourceMap.get(prestoQuery.getDbName());
    String sqlQuery = prestoQuery.getSql();
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sqlQuery);
      return fromSQLResultSet(rs, prestoQuery.getMetric(), prestoQuery.getGroupByKeys(), prestoQuery.getGranularity(),
          prestoQuery.getTimeSpec());
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
      throw new RuntimeException(e);
    } finally {
      conn.close();
    }
  }

}
