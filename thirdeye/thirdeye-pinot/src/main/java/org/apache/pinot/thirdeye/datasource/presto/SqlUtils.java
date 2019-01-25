package org.apache.pinot.thirdeye.datasource.presto;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.api.TimeGranularity;
import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.pinot.PqlUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqlUtils {

  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(", ");

  private static final String PREFIX_NOT_EQUALS = "!";
  private static final String PREFIX_LESS_THAN = "<";
  private static final String PREFIX_LESS_THAN_EQUALS = "<=";
  private static final String PREFIX_GREATER_THAN = ">";
  private static final String PREFIX_GREATER_THAN_EQUALS = ">=";

  private static final String OPERATOR_EQUALS = "IN";
  private static final String OPERATOR_NOT_EQUALS = "NOT IN";
  private static final String OPERATOR_LESS_THAN = "<";
  private static final String OPERATOR_LESS_THAN_EQUALS = "<=";
  private static final String OPERATOR_GREATER_THAN = ">";
  private static final String OPERATOR_GREATER_THAN_EQUALS = ">=";

  private static final Logger LOGGER = LoggerFactory.getLogger(PqlUtils.class);
  private static final int DEFAULT_LIMIT = 100000;
  private static final String PERCENTILE_TDIGEST_PREFIX = "percentileTDigest";


  /**
   * Returns sql to calculate the sum of all raw metrics required for <tt>request</tt>, grouped by
   * time within the requested date range. </br>
   * Due to the summation, all metric column values can be assumed to be doubles.
   * @throws ExecutionException
   */
  public static String getSql(ThirdEyeRequest request, MetricFunction metricFunction,
      Multimap<String, String> filterSet, TimeSpec dataTimeSpec) throws ExecutionException {
    // TODO handle request.getFilterClause()

    return getSql(metricFunction, request.getStartTimeInclusive(), request.getEndTimeExclusive(), filterSet,
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec, request.getLimit());
  }


  private static String getSql(MetricFunction metricFunction, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec, int limit) throws ExecutionException {

    MetricConfigDTO metricConfig = ThirdEyeUtils.getMetricConfigFromId(metricFunction.getMetricId());
    String dataset = metricFunction.getDataset();

    StringBuilder sb = new StringBuilder();

    boolean hasTime = false;
    if (timeGranularity != null && !groupBy.contains(timeGranularity)) {
      hasTime = true;
    }

    String selectionClause = getSelectionClause(metricConfig, metricFunction, groupBy, dataTimeSpec.getColumnName(), hasTime);

    String tableName = ThirdEyeUtils.computePrestoTableName(dataset);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(tableName);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec, dataset);
    sb.append(" WHERE ").append(betweenClause);


    String dimensionWhereClause = getDimensionWhereClause(filterSet);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }

    if (limit <= 0) {
      limit = DEFAULT_LIMIT;
    }

    String groupByClause = getDimensionGroupByClause(groupBy, timeGranularity, dataTimeSpec);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
      sb.append(" LIMIT " + limit);
    }

    return sb.toString();
  }

  private static String getSelectionClause(MetricConfigDTO metricConfig, MetricFunction metricFunction, List<String> groupByKeys, String timeColumn, boolean hasTime) {
    StringBuilder builder = new StringBuilder();

    if (hasTime) {
      builder.append(timeColumn).append(", ");
    }

    for (String groupByKey: groupByKeys) {
      builder.append(groupByKey).append(", ");
    }

    String metricName = null;
    if (metricFunction.getMetricName().equals("*")) {
      metricName = "*";
    } else {
      metricName = metricConfig.getName();
    }
    builder.append(convertAggFunction(metricFunction.getFunctionName())).append("(").append(metricName).append(")");

    return builder.toString();
  }



  private static String getMetricWhereClause(MetricConfigDTO metricConfig, MetricFunction metricFunction,
      List<String> metricNames, List<String> metricNamesColumns) {
    StringBuilder builder = new StringBuilder();
    if (!metricFunction.getMetricName().equals("*")) {
      for (int i = 0; i < metricNamesColumns.size(); i++) {
        String metricName = metricNames.get(i);
        String metricNamesColumn = metricNamesColumns.get(i);
        builder.append(" AND ");
        builder.append(String.format("%s='%s'", metricNamesColumn, metricName));
      }
    }
    return builder.toString();
  }


  static String getBetweenClause(DateTime start, DateTime endExclusive, TimeSpec timeFieldSpec, String dataset)
      throws ExecutionException {
    TimeGranularity dataGranularity = timeFieldSpec.getDataGranularity();
    long dataGranularityMillis = dataGranularity.toMillis();

    String timeField = timeFieldSpec.getColumnName();
    String timeFormat = timeFieldSpec.getFormat();

    // epoch case
    if (timeFormat == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
      long startUnits = (long) Math.ceil(start.getMillis() / (double) dataGranularityMillis);
      long endUnits = (long) Math.ceil(endExclusive.getMillis() / (double) dataGranularityMillis);

      // point query
      if (startUnits == endUnits) {
        return String.format(" %s = %d", timeField, startUnits);
      }

      return String.format(" %s BETWEEN %d AND %d", timeField, startUnits, endUnits);
    }

    // NOTE:
    // this is crazy. epoch rounds up, but timeFormat down
    // we maintain this behavior for backward compatibility.

    DateTimeFormatter inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(Utils.getDataTimeZone(dataset));
    String startUnits = inputDataDateTimeFormatter.print(start);
    String endUnits = inputDataDateTimeFormatter.print(endExclusive);

    // point query
    if (Objects.equals(startUnits, endUnits)) {
      return String.format(" %s = TIMESTAMP '%s'", timeField, startUnits);
    }

    return String.format(" %s BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'", timeField, startUnits, endUnits);
  }

  /**
   * Generates PQL WHERE clause for a given filter map. The supported operation are:
   * <pre>
   *   key, value (equals, <b>OR</b> semantics)
   *   key, !value (not equals, AND semantics)
   *   key, &gt;value (greater than, AND semantics)
   *   key, &gt;=value (greater than or equal, AND semantics)
   *   key, &lt;value (less than, AND semantics)
   *   key, &lt;=value (less than or equal, AND semantics)
   * </pre>
   *
   * @param dimensionValues multimap of filters
   * @return where-clause string
   */
  static String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<String> values = entry.getValue();
      if (values.isEmpty()) {
        continue;
      }

      // tokenize
      Set<String> greaterThanEquals = filter(values, PREFIX_GREATER_THAN_EQUALS);
      Set<String> greaterThan = filter(values, PREFIX_GREATER_THAN);
      Set<String> lessThanEquals = filter(values, PREFIX_LESS_THAN_EQUALS);
      Set<String> lessThen = filter(values, PREFIX_LESS_THAN);
      Set<String> notEquals = filter(values, PREFIX_NOT_EQUALS);
      Set<String> equals = new HashSet<>(values);

      // resolve ambiguity
      greaterThan.removeAll(greaterThanEquals);
      lessThen.removeAll(lessThanEquals);
      equals.removeAll(greaterThanEquals);
      equals.removeAll(greaterThan);
      equals.removeAll(lessThanEquals);
      equals.removeAll(lessThen);
      equals.removeAll(notEquals);

      // create components
      if (!equals.isEmpty()) {
        components.add(makeComponentGrouped(key, OPERATOR_EQUALS, equals));
      }
      if (!notEquals.isEmpty()) {
        components.add(makeComponentGrouped(key, OPERATOR_NOT_EQUALS, tokenize(PREFIX_NOT_EQUALS, notEquals)));
      }
      components.addAll(makeComponents(key, OPERATOR_GREATER_THAN, tokenize(PREFIX_GREATER_THAN, greaterThan)));
      components.addAll(makeComponents(key, OPERATOR_GREATER_THAN_EQUALS, tokenize(PREFIX_GREATER_THAN_EQUALS, greaterThanEquals)));
      components.addAll(makeComponents(key, OPERATOR_LESS_THAN, tokenize(PREFIX_LESS_THAN, lessThen)));
      components.addAll(makeComponents(key, OPERATOR_LESS_THAN_EQUALS, tokenize(PREFIX_LESS_THAN_EQUALS, lessThanEquals)));

    }

    if (components.isEmpty()) {
      return null;
    }

    Collections.sort(components);

    return AND.join(components);
  }

  private static String getDimensionGroupByClause(List<String> groupBy,
      TimeGranularity aggregationGranularity, TimeSpec timeSpec) {
    String timeColumnName = timeSpec.getColumnName();
    List<String> groups = new LinkedList<>();
    if (aggregationGranularity != null && !groups.contains(timeColumnName)) {
      groups.add(timeColumnName);
    }
      if (groupBy != null) {
        groups.addAll(groupBy);
      }
      if (groups.isEmpty()) {
        return "";
      }
      return String.format("GROUP BY %s", COMMA.join(groups));
  }

  /**
   * Surrounds a value with appropriate quote characters.
   *
   * @param value value to be quoted
   * @return quoted value
   * @throws IllegalArgumentException if no unused quote char can be found
   */
  static String quote(String value) {
    String quoteChar = "";
    if (!StringUtils.isNumeric(value)) {
      quoteChar = "\'";
      if (value.contains(quoteChar)) {
        throw new IllegalArgumentException(String.format("Could not find quote char for expression: %s", value));
      }
    }
    return String.format("%s%s%s", quoteChar, value, quoteChar);
  }

  /**
   * Convert the name of the MetricAggFunction to the name expected by Pinot. See PQL Documentation for details.
   *
   * @param aggFunction function enum to convert
   * @return a valid pinot function name
   */
  public static String convertAggFunction(MetricAggFunction aggFunction) {
    if (aggFunction.isPercentile()) {
      return aggFunction.name().replaceFirst(MetricAggFunction.PERCENTILE_PREFIX, PERCENTILE_TDIGEST_PREFIX);
    }
    return aggFunction.name();
  }

  /**
   * Returns a component with grouped values for a given key, operator, and values
   *
   * @param key key
   * @param operator operator
   * @param values values
   * @return grouped component
   */
  private static String makeComponentGrouped(String key, String operator, Collection<String> values) {
    List<String> quoted = new ArrayList<>();
    for (String value : values) {
      quoted.add(quote(value));
    }
    Collections.sort(quoted);
    return String.format("%s %s (%s)", key, operator, COMMA.join(quoted));
  }

  /**
   * Returns a set of components for a key, operator, and a collection of values.
   *
   * @param key key
   * @param operator operator
   * @param values collection of values
   * @return set of components
   */
  private static Set<String> makeComponents(String key, String operator, Collection<String> values) {
    Set<String> output = new HashSet<>();
    for (String value : values) {
      output.add(makeComponent(key, operator, value));
    }
    return output;
  }

  /**
   * Component for a key, operator and a value.
   *
   * @param key key
   * @param value raw value
   * @param operator  operator
   * @return pair of prefix, value
   */
  private static String makeComponent(String key, String operator, String value) {
    return String.format("%s %s %s", key, operator, quote(value));
  }

  /**
   * Tokenize a collection of values for a given prefix
   *
   * @param prefix prefix
   * @param values string values
   * @return set of tokenized values
   */
  private static Set<String> tokenize(String prefix, Collection<String> values) {
    Set<String> output = new HashSet<>();
    for (String value : values) {
      output.add(tokenize(prefix, value));
    }
    return output;
  }

  /**
   * Tokenize value for given prefix
   *
   * @param prefix prefix
   * @param value string value
   * @return tokenized value
   */
  private static String tokenize(String prefix, String value) {
    if (!value.startsWith(prefix)) {
      throw new IllegalArgumentException(String.format("Expected value with prefix '%s' but got '%s", prefix, value));
    }
    return value.substring(prefix.length());
  }

  /**
   * Filters a collection of strings for a given prefix
   *
   * @param values string values
   * @param prefix prefix
   * @return set of string with prefix
   */
  private static Set<String> filter(Collection<String> values, final String prefix) {
    return new HashSet<>(Collections2.filter(values, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String s) {
        return (s != null) && s.startsWith(prefix);
      }
    }));
  }
}

