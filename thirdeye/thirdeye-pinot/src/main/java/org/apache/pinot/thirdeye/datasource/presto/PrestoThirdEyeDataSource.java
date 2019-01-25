package org.apache.pinot.thirdeye.datasource.presto;
import com.google.common.collect.Multimap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetUtils;

import static org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeDataFrameResultSet.*;


public class PrestoThirdEyeDataSource implements ThirdEyeDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(PrestoThirdEyeDataSource.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final String PRESTO_URL = "prestoURLs";
  private static final String JDBC_USER = "user";
  private static final String JDBC_PASSWORD = "password";

  private Map<String, PrestoClient> dbNameToClientMap = new HashMap<>();
  private String user;
  private String password;

  public PrestoThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    Map<String, String> dbNameToURLMap = ConfigUtils.getMap(properties.get(PRESTO_URL));
    user = (String)properties.get(JDBC_USER);
    password = (String)properties.get(JDBC_PASSWORD);
    PrestoConfigLoader loader = new PrestoConfigLoader();
    for (Map.Entry<String, String> entry: dbNameToURLMap.entrySet()) {
      PrestoClient client = new PrestoClient(entry.getValue(), user, password);
      dbNameToClientMap.put(entry.getKey(), client);
    }
    loader.onboard();
  }

  @Override
  public String getName() {
    return PrestoThirdEyeDataSource.class.getSimpleName();
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    System.out.println(request);
    LinkedHashMap<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList = new LinkedHashMap<>();

    TimeSpec timeSpec = null;
    for (MetricFunction metricFunction : request.getMetricFunctions()) {
      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
      if (timeSpec == null) {
        timeSpec = dataTimeSpec;
      }
      Multimap<String, String> decoratedFilterSet = request.getFilterSet();

      String[] tableComponents = dataset.split("\\.");
      String dbName = tableComponents[0];

      String sqlQuery = SqlUtils.getSql(request, metricFunction, decoratedFilterSet, dataTimeSpec);
      ThirdEyeResultSet thirdEyeResultSet = executeSQL(sqlQuery, dbName, metricFunction, request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec);
      List<ThirdEyeResultSet> resultSetList = new ArrayList<>();
      resultSetList.add(thirdEyeResultSet);
      metricFunctionToResultSetList.put(metricFunction, resultSetList);
      List<String[]> resultRows = ThirdEyeResultSetUtils.parseResultSets(request, metricFunctionToResultSetList);
      return new PrestoThirdEyeResponse(request, resultRows, timeSpec);
    }

    return null;
  }

  public ThirdEyeResultSet executeSQL(String SqlQuery, String dbName, MetricFunction metricFunction, List<String> groupByKeys, TimeGranularity granularity, TimeSpec timeSpec) throws Exception {
    String metric = metricFunction.getMetricName();
    ThirdEyeResultSet thirdEyeResultSet = null;
    Connection conn = null;
    try {
      conn = dbNameToClientMap.get(dbName).getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(SqlQuery);
      System.out.println(SqlQuery);
      thirdEyeResultSet = fromSQLResultSet(rs, metric, groupByKeys, granularity, timeSpec);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      conn.close();
    }
    return thirdEyeResultSet;
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return CACHE_REGISTRY_INSTANCE.getDatasetsCache().getDatasets();
  }

  @Override
  public void clear() throws Exception {
    // left blank
  }

  @Override
  public void close() throws Exception {
    // left blank
  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
    DateTimeZone timeZone = Utils.getDataTimeZone(dataset);
    long maxTime = 0;

    String[] tableComponents = dataset.split("\\.");
    String dbName = tableComponents[0];
    String tableName = ThirdEyeUtils.computePrestoTableName(dataset);

    Connection conn = null;
    try {
      conn = dbNameToClientMap.get(dbName).getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT MAX(" + timeSpec.getColumnName() + ") FROM " + tableName);

      if (rs.next()) {
        String maxTimeString = rs.getString(1);
        String timeFormat = timeSpec.getFormat();

        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(Integer.valueOf(maxTimeString) - 1, timeZone);
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(timeZone);
          DateTime endDateTime = DateTime.parse(maxTimeString, inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      conn.close();
    }
    return maxTime;
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return null;
  }
}
