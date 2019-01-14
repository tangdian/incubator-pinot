package com.linkedin.thirdeye.datasource.presto;
//import com.google.common.base.Predicate;
//import com.google.common.collect.Collections2;
//import com.linkedin.thirdeye.dataframe.DataFrame;
//import com.linkedin.thirdeye.dataframe.StringSeries;
//import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
//import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
//import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
//import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
//import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.mock.AutoOnboardMockDataSource;
import com.linkedin.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.mock.MockThirdEyeDataSource;
import com.linkedin.thirdeye.detection.ConfigUtils;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.TimeUnit;
//import javax.annotation.Nullable;
//import org.apache.commons.collections.MapUtils;
//import org.apache.commons.lang.ArrayUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.math3.distribution.NormalDistribution;
//import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
//import org.joda.time.Period;
//import org.joda.time.PeriodType;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;

import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrestoThirdEyeDataSource implements ThirdEyeDataSource {
  final Map<String, MockDataset> datasets;
  private static final Logger LOG = LoggerFactory.getLogger(PrestoThirdEyeDataSource.class);
  private static final String PROP_POPULATE_META_DATA = "populateMetaData";
  private static final String PROP_DATASETS = "datasets";
  final Map<String, DataFrame> datasetData;
  final Map<Long, String> metricNameMap;
//  final Map<String, MockThirdEyeDataSource.MockDataset> datasets;
//
//
  final CSVThirdEyeDataSource delegate;

  public PrestoThirdEyeDataSource(Map<String, Object> properties) throws Exception {

    // datasets
    this.datasets = new HashMap<>();
    Map<String, Object> config = ConfigUtils.getMap(properties.get(PROP_DATASETS));
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      this.datasets.put(entry.getKey(), PrestoThirdEyeDataSource.MockDataset.fromMap(
          entry.getKey(), ConfigUtils.<String, Object>getMap(entry.getValue())
      ));
    }

    // auto onboarding support
    if (MapUtils.getBooleanValue(properties, PROP_POPULATE_META_DATA, false)) {
      MetadataSourceConfig metadataSourceConfig = new MetadataSourceConfig();
      metadataSourceConfig.setProperties(properties);

      AutoOnboardMockDataSource onBoarding = new AutoOnboardMockDataSource(metadataSourceConfig);

      onBoarding.runAdhoc();
    }

    // merge data
    long metricNameCounter = 0;
    this.datasetData = new HashMap<>();
    this.metricNameMap = new HashMap<>();
    this.metricNameMap.put(1L, "discount");
    this.metricNameMap.put(2L, "extendedprice");


    List<String> indexes = new ArrayList<>();
    List<String> metrics = Arrays.asList("extendedprice", "discount");
    List<String> dimensions = Arrays.asList("returnflag", "linenumber", "linestatus");
    indexes.add("timestamp");
    indexes.addAll(dimensions);

    DataFrame df = new DataFrame();


    String url = "jdbc:presto://presto.grid.linkedin.com:8443/hive?SSL=true";
    Properties connprop = new Properties();
    connprop.setProperty("user", "ditang");
    connprop.setProperty("password", "Wdlinkedin2907!403667");
    try {
      Connection connection = DriverManager.getConnection(url, connprop);
      System.out.println("GOODJOB");
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("select returnflag, linenumber, linestatus, extendedprice, discount, commitdate from tpch.tiny.lineitem ORDER BY commitdate DESC limit 100");
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsNumber = rsmd.getColumnCount();

      Map<String, List<Object>> columns = new HashMap<>();

      while (rs.next()) {
        for (int i = 1; i <= columnsNumber; i++) {
          String columnName = rsmd.getColumnName(i);
          if (!columns.containsKey(columnName)) {
            columns.put(columnName, new ArrayList<>());
          }
          if (columnName.equals("commitdate")) {
            Date d = rs.getDate(i);
            columns.get(columnName).add(rs.getDate(i).getTime() + 630700000000L);
          } else if (metrics.contains(columnName)) {
            columns.get(columnName).add(rs.getDouble(i));
          } else if (dimensions.contains(columnName)) {
            columns.get(columnName).add(rs.getString(i));
          }
        }
      }

      for (Map.Entry<String, List<Object>> entry: columns.entrySet()) {
        if (entry.getKey().equals("commitdate")) {
          df.addSeries("timestamp", ArrayUtils.toPrimitive(entry.getValue().toArray(new Long[0])));
        } else if (metrics.contains(entry.getKey())) {
          df.addSeries(entry.getKey(), ArrayUtils.toPrimitive(entry.getValue().toArray(new Double[0])));
        } else if (dimensions.contains(entry.getKey())) {
          df.addSeries(entry.getKey(), entry.getValue().toArray(new String[0]));
        }
      }

      df.setIndex(indexes);

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    this.datasetData.put("tpch", df);
    this.delegate = CSVThirdEyeDataSource.fromDataFrame(this.datasetData, this.metricNameMap);
    System.out.println(df);
  }

  @Override
  public String getName() {
    return PrestoThirdEyeDataSource.class.getSimpleName();
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    return this.delegate.execute(request);
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return new ArrayList<>(this.datasets.keySet());
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
    return this.delegate.getMaxDataTime(dataset);
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return null;
  }

  /**
   * Container class for datasets and their generator configs
   */
  static final class MockDataset {
    final String name;
    final DateTimeZone timezone;
    final List<String> dimensions;
    final Map<String, Map<String, Object>> metrics;
    final Period granularity;

    MockDataset(String name, DateTimeZone timezone, List<String> dimensions, Map<String, Map<String, Object>> metrics, Period granularity) {
      this.name = name;
      this.timezone = timezone;
      this.dimensions = dimensions;
      this.metrics = metrics;
      this.granularity = granularity;
    }

    static PrestoThirdEyeDataSource.MockDataset fromMap(String name, Map<String, Object> map) {
      return new PrestoThirdEyeDataSource.MockDataset(
          name,
          DateTimeZone.forID(MapUtils.getString(map, "timezone", "America/Los_Angeles")),
          ConfigUtils.<String>getList(map.get("dimensions")),
          ConfigUtils.<String, Map<String, Object>>getMap(map.get("metrics")),
          ConfigUtils.parsePeriod(MapUtils.getString(map, "granularity", "1day")));
    }
  }

}
