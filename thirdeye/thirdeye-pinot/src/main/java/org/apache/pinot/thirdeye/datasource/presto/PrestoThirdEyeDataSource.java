package org.apache.pinot.thirdeye.datasource.presto;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetUtils;



public class PrestoThirdEyeDataSource implements ThirdEyeDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(PrestoThirdEyeDataSource.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  protected LoadingCache<PrestoQuery, ThirdEyeResultSet> prestoResponseCache;
  private PrestoResponseCacheLoader prestoResponseCacheLoader;
  public static final String DATA_SOURCE_NAME = PrestoThirdEyeDataSource.class.getSimpleName();

  // TODO: make default cache size configurable
  private static final int DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE = 50;
  private static final int DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 100;
  private static final int DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 8192;

  public PrestoThirdEyeDataSource(Map<String, Object> properties) throws Exception {
    prestoResponseCacheLoader = new PrestoResponseCacheLoader(properties);

    prestoResponseCache = buildResponseCache(prestoResponseCacheLoader);
    PrestoConfigLoader loader = new PrestoConfigLoader();
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

      String[] tableComponents = dataset.split("\\.");
      String dbName = tableComponents[0];

      String sqlQuery = SqlUtils.getSql(request, metricFunction, request.getFilterSet(), dataTimeSpec);
      System.out.println(sqlQuery);
      ThirdEyeResultSet thirdEyeResultSet = executeSQL(new PrestoQuery(sqlQuery, dbName,
          metricFunction.getMetricName(), request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec));

      List<ThirdEyeResultSet> resultSetList = new ArrayList<>();
      resultSetList.add(thirdEyeResultSet);

      metricFunctionToResultSetList.put(metricFunction, resultSetList);
      List<String[]> resultRows = ThirdEyeResultSetUtils.parseResultSets(request, metricFunctionToResultSetList);

      return new PrestoThirdEyeResponse(request, resultRows, timeSpec);
    }

    return null;
  }

  public ThirdEyeResultSet executeSQL(PrestoQuery prestoQuery) throws Exception {
    ThirdEyeResultSet thirdEyeResultSet = null;
    try {
      thirdEyeResultSet = prestoResponseCache.get(prestoQuery);
    } catch (Exception e) {
      throw new RuntimeException(e);
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
    System.out.println("Getting max data time");
    return prestoResponseCacheLoader.getMaxDataTime(dataset);
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    System.out.println("Running dimension filters");
    return this.prestoResponseCacheLoader.getDimensionFilters(dataset);
  }

  private static LoadingCache<PrestoQuery, ThirdEyeResultSet> buildResponseCache(
      PrestoResponseCacheLoader prestoResponseCacheLoader) throws Exception {
    Preconditions.checkNotNull(prestoResponseCacheLoader, "A loader that sends query to Pinot is required.");

    // Initializes listener that prints expired entries in debuggin mode.
    RemovalListener<PrestoQuery, ThirdEyeResultSet> listener;
    if (LOG.isDebugEnabled()) {
      listener = new RemovalListener<PrestoQuery, ThirdEyeResultSet>() {
        @Override
        public void onRemoval(RemovalNotification<PrestoQuery, ThirdEyeResultSet> notification) {
          LOG.debug("Expired {}", notification.getKey().getSql());
        }
      };
    } else {
      listener = new RemovalListener<PrestoQuery, ThirdEyeResultSet>() {
        @Override public void onRemoval(RemovalNotification<PrestoQuery, ThirdEyeResultSet> notification) { }
      };
    }

    // ResultSetGroup Cache. The size of this cache is limited by the total number of buckets in all ResultSetGroup.
    // We estimate that 1 bucket (including overhead) consumes 1KB and this cache is allowed to use up to 50% of max
    // heap space.
    long maxBucketNumber = getApproximateMaxBucketNumber(DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE);
    LOG.debug("Max bucket number for {}'s cache is set to {}", DATA_SOURCE_NAME, maxBucketNumber);

    return CacheBuilder.newBuilder()
        .removalListener(listener)
        .expireAfterWrite(15, TimeUnit.MINUTES)
        .maximumWeight(maxBucketNumber)
        .weigher(new Weigher<PrestoQuery, ThirdEyeResultSet>() {
          @Override public int weigh(PrestoQuery prestoQuery, ThirdEyeResultSet resultSet) {
              return ((resultSet.getColumnCount() + resultSet.getGroupKeyLength()) * resultSet.getRowCount());
          }
        })
        .build(prestoResponseCacheLoader);
  }

  private static long getApproximateMaxBucketNumber(int percentage) {
    long jvmMaxMemoryInBytes = Runtime.getRuntime().maxMemory();
    if (jvmMaxMemoryInBytes == Long.MAX_VALUE) { // Check upper bound
      jvmMaxMemoryInBytes = DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
    } else { // Check lower bound
      long lowerBoundInBytes = DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
      if (jvmMaxMemoryInBytes < lowerBoundInBytes) {
        jvmMaxMemoryInBytes = lowerBoundInBytes;
      }
    }
    return (jvmMaxMemoryInBytes / 102400) * percentage;
  }
}