package org.apache.pinot.thirdeye.datasource.pinot.resultset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.TimeRangeUtils;
import org.apache.pinot.thirdeye.datasource.presto.PrestoThirdEyeDataSource;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeResultSetUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeResultSetUtils.class);

  public static List<String[]> parseResultSets(ThirdEyeRequest request,
      Map<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList) throws ExecutionException {

    int numGroupByKeys = 0;
    boolean hasGroupBy = false;
    if (request.getGroupByTimeGranularity() != null) {
      numGroupByKeys += 1;
    }
    if (request.getGroupBy() != null) {
      numGroupByKeys += request.getGroupBy().size();
    }
    if (numGroupByKeys > 0) {
      hasGroupBy = true;
    }
    int numMetrics = request.getMetricFunctions().size();
    int numCols = numGroupByKeys + numMetrics;
    boolean hasGroupByTime = false;
    if (request.getGroupByTimeGranularity() != null) {
      hasGroupByTime = true;
    }

    int position = 0;
    Map<String, String[]> dataMap = new HashMap<>();
    Map<String, Integer> countMap = new HashMap<>();
    for (Map.Entry<MetricFunction, List<ThirdEyeResultSet>> entry : metricFunctionToResultSetList.entrySet()) {

      MetricFunction metricFunction = entry.getKey();

      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

      TimeGranularity dataGranularity = null;
      long startTime = request.getStartTimeInclusive().getMillis();
      DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
      DateTime startDateTime = new DateTime(startTime, dateTimeZone);
      dataGranularity = dataTimeSpec.getDataGranularity();
      boolean isISOFormat = false;
      DateTimeFormatter inputDataDateTimeFormatter = null;
      String timeFormat = dataTimeSpec.getFormat();
      if (timeFormat != null && !timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
        isISOFormat = true;
        inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dateTimeZone);
      }

      List<ThirdEyeResultSet> resultSets = entry.getValue();
      for (int i = 0; i < resultSets.size(); i++) {
        ThirdEyeResultSet resultSet = resultSets.get(i);
//        System.out.println(resultSet);
        int numRows = resultSet.getRowCount();
        for (int r = 0; r < numRows; r++) {
          boolean skipRowDueToError = false;
          String[] groupKeys;
          if (hasGroupBy) {
            groupKeys = new String[resultSet.getGroupKeyLength()];
            for (int grpKeyIdx = 0; grpKeyIdx < resultSet.getGroupKeyLength(); grpKeyIdx++) {
              String groupKeyVal = "";
              try {
                groupKeyVal = resultSet.getGroupKeyColumnValue(r, grpKeyIdx);
              } catch (Exception e) {
                // IGNORE FOR NOW, workaround for Pinot Bug
              }
              if (hasGroupByTime && grpKeyIdx == 0) {
                int timeBucket;
                long millis;
                if (!isISOFormat) {
                  millis = dataGranularity.toMillis(Double.valueOf(groupKeyVal).longValue());
                } else {
                  millis = DateTime.parse(groupKeyVal, inputDataDateTimeFormatter).getMillis();
                }
                if (millis < startTime) {
                  LOG.error("Data point earlier than requested start time {}: {}", new Date(startTime), new Date(millis));
                  skipRowDueToError = true;
                  break;
                }
                timeBucket = TimeRangeUtils
                    .computeBucketIndex(request.getGroupByTimeGranularity(), startDateTime,
                        new DateTime(millis, dateTimeZone));
                groupKeyVal = String.valueOf(timeBucket);
              }
              groupKeys[grpKeyIdx] = groupKeyVal;
            }
            if (skipRowDueToError) {
              continue;
            }
          } else {
            groupKeys = new String[] {};
          }
          String compositeGroupKey = StringUtils.join(groupKeys, "|");

          String[] rowValues = dataMap.get(compositeGroupKey);
          if (rowValues == null) {
            rowValues = new String[numCols];
            Arrays.fill(rowValues, "0");
            System.arraycopy(groupKeys, 0, rowValues, 0, groupKeys.length);
            dataMap.put(compositeGroupKey, rowValues);
          }

          String countKey = compositeGroupKey + "|" + position;
          if (!countMap.containsKey(countKey)) {
            countMap.put(countKey, 0);
          }
          final int aggCount = countMap.get(countKey);
          countMap.put(countKey, aggCount + 1);

          // aggregation of multiple values
          rowValues[groupKeys.length + position + i] = String.valueOf(
              reduce(
                  Double.parseDouble(rowValues[groupKeys.length + position + i]),
                  Double.parseDouble(resultSet.getString(r, 0)),
                  aggCount,
                  metricFunction.getFunctionName()
              ));

        }
      }
      position ++;
    }
    List<String[]> rows = new ArrayList<>();
    rows.addAll(dataMap.values());
    return rows;
  }

  public static double reduce(double aggregate, double value, int prevCount, MetricAggFunction aggFunction) {
    if (aggFunction.equals(MetricAggFunction.SUM)) {
      return aggregate + value;
    } else if (aggFunction.equals(MetricAggFunction.AVG) || aggFunction.isPercentile()) {
      return (aggregate * prevCount + value) / (prevCount + 1);
    } else if (aggFunction.equals(MetricAggFunction.MAX)) {
      return Math.max(aggregate, value);
    } else if (aggFunction.equals(MetricAggFunction.COUNT)) {
      return aggregate + 1;
    } else {
      throw new IllegalArgumentException(String.format("Unknown aggregation function '%s'", aggFunction));
    }
  }
}
