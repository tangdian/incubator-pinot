package org.apache.pinot.thirdeye.datasource.presto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PrestoDatasetConfigs {
  @JsonProperty
  private Map<String, PrestoDataset> prestoSourcesConfigs = Maps.newLinkedHashMap();

  public Map<String, PrestoDataset> getPrestoSourcesConfigs() {
    return prestoSourcesConfigs;
  }
}
