package org.apache.pinot.thirdeye.detection.components;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.algorithm.AlgorithmUtils;
import org.apache.pinot.thirdeye.detection.spec.HoltWintersDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Interval;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class HoltWintersDetectorTest {

  private DataProvider provider;
  private DataFrame data;

  @BeforeMethod
  public void setUp() throws Exception {
    try (Reader dataReader = new InputStreamReader(AlgorithmUtils.class.getResourceAsStream("daily.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
    }

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(1L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(1);

    Map<MetricSlice, DataFrame> timeseries = new HashMap<>();
    timeseries.put(MetricSlice.from(1L, 1391819600000L, 1399590000000L), this.data);
    timeseries.put(MetricSlice.from(1L, 1397000000000L, 1399590000000L), this.data);
    this.provider = new MockDataProvider()
        .setTimeseries(timeseries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
  }

  @Test
  public void testComputePredictedTimeSeries() {
    HoltWintersDetector detector = new HoltWintersDetector();
    HoltWintersDetectorSpec spec = new HoltWintersDetectorSpec();
//    spec.setPeriod(7);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    Interval window = new Interval(1397000000000L, 1399590000000L);
    String metricUrn = "thirdeye:metric:1";
    MetricEntity me = MetricEntity.fromURN(metricUrn);

    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters());

    TimeSeries timeSeries = detector.computePredictedTimeSeries(slice);

  }

  @Test
  public void testRunDetectionDaily() {
    HoltWintersDetector detector = new HoltWintersDetector();
    HoltWintersDetectorSpec spec = new HoltWintersDetectorSpec();
//    spec.setPeriod(7);
    detector.init(spec, new DefaultInputDataFetcher(this.provider, -1));
    Interval window = new Interval(1397000000000L, 1399590000000L);
    String metricUrn = "thirdeye:metric:1";
    MetricEntity me = MetricEntity.fromURN(metricUrn);

    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters());

    List<MergedAnomalyResultDTO> result = detector.runDetection(window, metricUrn);
  }
}