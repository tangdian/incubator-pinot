package org.apache.pinot.thirdeye.datasource.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.joda.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrestoConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PrestoConfigLoader.class);
  private final String CONFIG_PATH = System.getProperty("dw.rootDir") + "/data-sources/presto-sources-config.yml";
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;

  public PrestoConfigLoader() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  public void onboard() {
    List<DatasetConfigDTO> datasetConfigs = new ArrayList<>();
    List<MetricConfigDTO> metricConfigs = new ArrayList<>();

    PrestoDatasetConfigs yaml = null;
    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      yaml = mapper.readValue(new File(CONFIG_PATH), PrestoDatasetConfigs.class);
      for (Map.Entry<String, PrestoDataset> entry: yaml.getPrestoSourcesConfigs().entrySet()) {
        String datasetName = entry.getKey();
        PrestoDataset dataset = entry.getValue();
        List<String> sortedDimensions = dataset.getDimensions();
        Collections.sort(sortedDimensions);

        Period granularity = ConfigUtils.parsePeriod(dataset.getGranularity());

        DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
        datasetConfig.setDataset(datasetName);
        datasetConfig.setDataSource(PrestoThirdEyeDataSource.class.getSimpleName());
        datasetConfig.setDimensions(sortedDimensions);
        datasetConfig.setTimezone(dataset.getTimezone());
        datasetConfig.setTimeDuration(getTimeDuration(granularity));
        datasetConfig.setTimeUnit(getTimeUnit(granularity));
        datasetConfig.setTimeColumn(dataset.getTimeColumn());
        datasetConfig.setTimeFormat(dataset.getTimeFormat());

        datasetConfigs.add(datasetConfig);

        List<String> sortedMetrics = new ArrayList<>(dataset.getMetrics().keySet());

        Collections.sort(sortedMetrics);

        for (Map.Entry<String, MetricAggFunction> metric: dataset.getMetrics().entrySet()) {
          MetricConfigDTO metricConfig = new MetricConfigDTO();
          metricConfig.setName(metric.getKey());
          metricConfig.setDataset(datasetName);
          metricConfig.setAlias(String.format("%s::%s", datasetName, metric.getKey()));
          metricConfig.setDefaultAggFunction(metric.getValue());
          metricConfigs.add(metricConfig);
        }
      }

      LOG.info("Read {} datasets and {} metrics", datasetConfigs.size(), metricConfigs.size());

      // NOTE: save in order as mock datasource expects metric ids first
      for (MetricConfigDTO metricConfig : metricConfigs) {
        Long id = this.metricDAO.save(metricConfig);
        if (id != null) {
          LOG.info("Created metric '{}' with id {}", metricConfig.getAlias(), id);
        } else {
          LOG.warn("Could not create metric '{}'", metricConfig.getAlias());
        }
      }

      for (DatasetConfigDTO datasetConfig : datasetConfigs) {
        Long id = this.datasetDAO.save(datasetConfig);
        if (id != null) {
          LOG.info("Created dataset '{}' with id {}", datasetConfig.getDataset(), id);
        } else {
          LOG.warn("Could not create dataset '{}'", datasetConfig.getDataset());
        }
      }
    } catch (Exception e) {
      System.out.println(e);
    }


  }
  /**
   * Guess time duration from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  private static int getTimeDuration(Period granularity) {
    if (granularity.getDays() > 0) {
      return granularity.getDays();
    }
    if (granularity.getHours() > 0) {
      return granularity.getHours();
    }
    if (granularity.getMinutes() > 0) {
      return granularity.getMinutes();
    }
    if (granularity.getSeconds() > 0) {
      return granularity.getSeconds();
    }
    return granularity.getMillis();
  }

  /**
   * Guess time unit from period.
   *
   * @param granularity dataset granularity
   * @return
   */
  private static TimeUnit getTimeUnit(Period granularity) {
    if (granularity.getDays() > 0) {
      return TimeUnit.DAYS;
    }
    if (granularity.getHours() > 0) {
      return TimeUnit.HOURS;
    }
    if (granularity.getMinutes() > 0) {
      return TimeUnit.MINUTES;
    }
    if (granularity.getSeconds() > 0) {
      return TimeUnit.SECONDS;
    }
    return TimeUnit.MILLISECONDS;
  }
}
