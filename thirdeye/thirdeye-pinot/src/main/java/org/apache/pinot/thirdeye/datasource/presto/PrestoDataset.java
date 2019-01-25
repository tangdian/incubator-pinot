package org.apache.pinot.thirdeye.datasource.presto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;


public class PrestoDataset {

  @JsonProperty
  private String timeColumn;
  @JsonProperty
  private List<String> dimensions;
  @JsonProperty
  private Map<String, MetricAggFunction> metrics;
  @JsonProperty
  private String granularity = "1hour";
  @JsonProperty
  private String timezone = "America/Los_Angeles";

  @JsonProperty
  private String timeFormat = "EPOCH";

  public String getTimeColumn() {
    return timeColumn;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public Map<String, MetricAggFunction> getMetrics() {
    return metrics;
  }

  public String getGranularity() {
    return granularity;
  }

  public String getTimezone() {
    return timezone;
  }

  public String getTimeFormat() {
    return timeFormat;
  }


}
