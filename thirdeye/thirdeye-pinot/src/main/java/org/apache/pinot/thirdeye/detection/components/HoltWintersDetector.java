package org.apache.pinot.thirdeye.detection.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.spec.HoltWintersDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;


import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.special.Erf;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


@Components(title = "Holt Winters triple exponential smoothing forecasting and detection",
    type = "HOLT_WINTERS_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    description = "Forecast with holt winters triple exponential smoothing and generate anomalies",
    params = {
        @Param(name = "alpha"),
        @Param(name = "beta"),
        @Param(name = "gamma"),
        @Param(name = "pattern")
    })
public class HoltWintersDetector implements BaselineProvider<HoltWintersDetectorSpec>,
                                            AnomalyDetector<HoltWintersDetectorSpec> {
  private InputDataFetcher dataFetcher;
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_DIFF = "diff";
  private static final String COL_DIFF_VIOLATION = "diff_violation";
  private static final String COL_ERROR = "error";
  private static final String COL_SSE = "sse";
  private static final String TRUE = "true";
  private static final String MULTIPLICATIVE = "multiplicative";
  private static final String ADDITIVE = "ADDITIVE";

  private static final int YEARLY_PERIOD = 365;
  private static final int YEARLY_LOOKBACK = 1096;

  private int period;

  private double alpha;
  private double beta;
  private double gamma;
  private boolean yearly = false;
  private Pattern pattern;
  private int lookback;
  private String method;
  private TimeGranularity timeGranularity;

  private double zscore;
  private String monitoringGranularity;

  private Period lookbackPeriod;

  @Override
  public void init(HoltWintersDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.period = spec.getPeriod();
    this.alpha = spec.getAlpha();
    this.beta = spec.getBeta();
    this.gamma = spec.getGamma();
    this.method = spec.getMethod();
    String yearlyConfig = spec.getYearly();
    if (yearlyConfig.equalsIgnoreCase(TRUE)) {
      this.yearly = true;
    }

    System.out.println("alpha is: " + alpha + " beta is: " + beta + " gamma is: " + gamma);
    System.out.println(method);
    this.dataFetcher = dataFetcher;
    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());
    this.lookback = spec.getLookback();
    if (yearly) {
      this.lookback = YEARLY_LOOKBACK;
    }

    this.zscore = pValueToZscore(spec.getPvalue());
    System.out.println(zscore);
    this.monitoringGranularity = spec.getMonitoringGranularity();
    lookbackPeriod = ConfigUtils.parsePeriod(this.lookback + "DAYS");
    if (this.monitoringGranularity.equals("1_MONTHS")) {
      this.timeGranularity = MetricSlice.NATIVE_GRANULARITY;
    } else {
      this.timeGranularity = TimeGranularity.fromString(spec.getMonitoringGranularity());
    }
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    MetricEntity metricEntity = MetricEntity.fromSlice(slice, 0);
    Interval window = new Interval(slice.getStart(), slice.getEnd());

    DateTime trainStart = new DateTime(window.getStart()).minus(lookbackPeriod);
    MetricSlice sliceData = MetricSlice.from(metricEntity.getId(), trainStart.getMillis(), window.getEndMillis(), metricEntity.getFilters(), timeGranularity);
    List<MetricSlice> slices = new ArrayList<>();
    slices.add(sliceData);
    InputDataSpec dataSpec = new InputDataSpec().withTimeseriesSlices(slices)
        .withMetricIdsForDataset(Collections.singletonList(sliceData.getMetricId()));
    InputData inputData = this.dataFetcher.fetchData(dataSpec);
    DataFrame inputDf = inputData.getTimeseries().get(sliceData).sortedBy(COL_TIME);
    DataFrame resultDF = computePredictedValueAndErrorDF(inputDf,
        window.getStartMillis(), period, alpha, beta, gamma);

    // Exclude the end
    if (resultDF.size() > 1) {
      resultDF = resultDF.head(resultDF.size() - 1);
    }

    return TimeSeries.fromDataFrame(resultDF);
  }



  @Override
  public List<MergedAnomalyResultDTO> runDetection(Interval window, String metricUrn) {

    MetricEntity metricEntity = MetricEntity.fromURN(metricUrn);
    DateTime trainStart = new DateTime(window.getStart()).minus(lookbackPeriod);
    MetricSlice sliceData = MetricSlice.from(metricEntity.getId(), trainStart.getMillis(), window.getEndMillis(), metricEntity.getFilters(), timeGranularity);
    List<MetricSlice> slices = new ArrayList<>();
    slices.add(sliceData);
    InputDataSpec dataSpec = new InputDataSpec().withTimeseriesSlices(slices)
        .withMetricIdsForDataset(Collections.singletonList(sliceData.getMetricId()));
    InputData inputData = this.dataFetcher.fetchData(dataSpec);
    DataFrame inputDf = inputData.getTimeseries().get(sliceData).sortedBy(COL_TIME);

    DataFrame dfCurr = new DataFrame(inputDf).renameSeries(COL_VALUE, COL_CURR);

    DataFrame dfBase = computePredictedValueAndErrorDF(inputDf, window.getStartMillis(),
        period, alpha, beta, gamma).renameSeries(COL_VALUE, COL_BASE);

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);

    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(COL_BASE)));

    // defaults
    df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false));

    // consistent with pattern
    if (pattern.equals(Pattern.UP_OR_DOWN) ) {
      df.addSeries(COL_PATTERN, BooleanSeries.fillValues(df.size(), true));
    } else {
      df.addSeries(COL_PATTERN, pattern.equals(Pattern.UP) ? df.getDoubles(COL_DIFF).gt(0) :
          df.getDoubles(COL_DIFF).lt(0));
    }
    df.addSeries(COL_DIFF_VIOLATION, df.getDoubles(COL_DIFF).abs().gte(df.getDoubles(COL_ERROR)));
    df.mapInPlace(BooleanSeries.ALL_TRUE, COL_ANOMALY, COL_PATTERN, COL_DIFF_VIOLATION);

    System.out.println(df.dropNull());
    // anomalies

    DatasetConfigDTO datasetConfig = inputData.getDatasetForMetricId().get(metricEntity.getId());
    List<MergedAnomalyResultDTO> result = DetectionUtils.makeAnomalies(sliceData, df, COL_ANOMALY,
        window.getEndMillis(),
        DetectionUtils.getMonitoringGranularityPeriod(monitoringGranularity, datasetConfig), datasetConfig);

    return result;
  }

  private static double calculateInitialLevel(double[] y) {
    return y[0];
  }

  /**
   * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
   *
   * @return - Initial trend - Bt[1]
   */
  private static double calculateInitialTrend(double[] y, int period) {
    double sum = 0;

    for (int i = 0; i < period; i++) {
      sum += (y[period + i] - y[i]);
    }

    return sum / (period * period);
  }

  /**
   * See: http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
   *
   * @return - Seasonal Indices.
   */
  private static double[] calculateSeasonalIndices(double[] y, int period,
      int seasons) {

    double[] seasonalAverage = new double[seasons];
    double[] seasonalIndices = new double[period];

    double[] averagedObservations = new double[y.length];

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        seasonalAverage[i] += y[(i * period) + j];
      }
      seasonalAverage[i] /= period;
    }

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        averagedObservations[(i * period) + j] = y[(i * period) + j]
            / seasonalAverage[i];
      }
    }

    for (int i = 0; i < period; i++) {
      for (int j = 0; j < seasons; j++) {
        seasonalIndices[i] += averagedObservations[(j * period) + i];
      }
      seasonalIndices[i] /= seasons;
    }

    return seasonalIndices;
  }

  private double[] forecast(double[] y, double alpha, double beta, double gamma, double omega) {
    double[] It = new double[y.length+1];
    double[] Wt = new double[y.length+1];
    double[] Ft = new double[y.length+1];


    double a0 = calculateInitialLevel(y);
    double b0 = calculateInitialTrend(y, period);

    int seasons = y.length / period;
    double[] initialSeasonalIndices = calculateSeasonalIndices(y, period,
        seasons);

    for (int i = 0; i < period; i++) {
      It[i] = initialSeasonalIndices[i];
    }

    if (yearly) {
      int seasonsLong = y.length / YEARLY_PERIOD;
      double[] initialSeasonalIndicesLong = calculateSeasonalIndices(y, YEARLY_PERIOD, seasonsLong);
      for (int i = 0; i < YEARLY_PERIOD; i++) {
        Wt[i] = initialSeasonalIndicesLong[i] / It[i % period];
      }
    }

    double s = a0;
    double t = b0;
    double predictedValue = 0;

    for (int i = 0; i < y.length; i++) {
      double sNew;
      double tNew;
      if (yearly) {
        if (method.equalsIgnoreCase(MULTIPLICATIVE)) {
          Ft[i] = (s + t) * It[i] * Wt[i];

          sNew = alpha * (y[i] / (It[i] * Wt[i])) + (1 - alpha) * (s + t);
          tNew = beta * (sNew - s) + (1 - beta) * t;
          if (i + period <= y.length) {
            It[i + period] = gamma * (y[i] / (sNew * It[i])) + (1 - gamma) * It[i];
          }
          if (i + YEARLY_PERIOD <= y.length) {
            Wt[i + YEARLY_PERIOD] = omega * (y[i] / (sNew * Wt[i])) + (1 - omega) * Wt[i];
          }
          s = sNew;
          t = tNew;
          if (i == y.length-1) {
            predictedValue = (s+t) * It[i+1] * Wt[i+1];
          }

        } else if (method.equalsIgnoreCase(ADDITIVE)) {
          Ft[i] = (s + t) + It[i] + Wt[i];
          sNew = alpha * (y[i] - (It[i] + Wt[i])) + (1 - alpha) * (s + t);
          tNew = beta * (sNew - s) + (1 - beta) * t;
          if (i + period <= y.length) {
            It[i + period] = gamma * (y[i] - (sNew + It[i])) + (1 - gamma) * It[i];
          }
          if (i + YEARLY_PERIOD <= y.length) {
            Wt[i + YEARLY_PERIOD] = omega * (y[i] - (sNew + Wt[i])) + (1 - omega) * Wt[i];
          }
          s = sNew;
          t = tNew;
          if (i == y.length-1) {
            predictedValue = (s+t) + It[i+1] + Wt[i+1];
          }
        }
      } else {
        if (method.equalsIgnoreCase(MULTIPLICATIVE)) {
          Ft[i] = (s + t) * It[i];
          sNew = alpha * (y[i] / It[i]) + (1 - alpha) * (s + t);
          tNew = beta * (sNew - s) + (1 - beta) * t;
          if (i + period <= y.length) {
            It[i + period] = gamma * (y[i] / (sNew * It[i])) + (1 - gamma) * It[i];
          }

          s = sNew;
          t = tNew;
          if (i == y.length-1) {
            predictedValue = (s+t) * It[i+1];
          }
        } else if (method.equalsIgnoreCase(ADDITIVE)) {
          Ft[i] = (s + t) + It[i];
          sNew = alpha * (y[i] - It[i]) + (1 - alpha) * (s + t);
          tNew = beta * (sNew - s) + (1 - beta) * t;
          if (i + period <= y.length) {
            It[i + period] = gamma * (y[i] - (sNew + It[i])) + (1 - gamma) * It[i];
          }
          s = sNew;
          t = tNew;
          if (i == y.length-1) {
            predictedValue = (s+t) + It[i+1];
          }
        }
      }
    }
    List<Double> diff = new ArrayList<>();
    double sse = 0;
    for (int i = 0; i < y.length; i++) {
      if (Ft[i] != 0) {
        sse += Math.pow(y[i] - Ft[i], 2);
        diff.add(Math.abs(Ft[i] - y[i]));
      }
    }

    double error = calculateLowerUpperConfidenceBoundary(diff, zscore);

    return new double[]{predictedValue, sse, error};
  }

  private DataFrame computePredictedValueAndErrorDF (DataFrame inputDF, long windowStartTime, int period,
      double alpha, double beta, double gamma) {

    DataFrame resultDF = new DataFrame();

    DataFrame forecastDF = inputDF.filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= windowStartTime;
      }
    }, COL_TIME).dropNull();

    int size = forecastDF.size();

    double[] resultArray = new double[size];
    long[] resultTimeArray = new long[size];
    double[] errorArray = new double[size];
    double[] sseArray = new double[size];

    double lastAlpha = -1;
    double lastBeta = -1;
    double lastGamma = -1;
    double lastOmega = -1;

    for (int k = 0; k < size; k++) {

      DataFrame dailyDF = getDailyDFFromSubDailyPoint(inputDF, forecastDF.getLong(COL_TIME, k), lookback);

      // We need at least 2 periods of data
      if (dailyDF.size() < 2 * period || (yearly) && dailyDF.size() < 2 * YEARLY_PERIOD) {
        continue;
      }

      resultTimeArray[k] = forecastDF.getLong(COL_TIME, k);

      double[] y = dailyDF.getDoubles(COL_VALUE).values();
      double[] params;
      if (alpha < 0 && beta < 0 && gamma < 0) {
        params = fitModelWithBOBYQA(y, lastAlpha, lastBeta, lastGamma, lastOmega);
      } else {
        params = new double[3];
        params[0] = alpha;
        params[1] = beta;
        params[2] = gamma;
      }
      lastAlpha = params[0];
      lastBeta = params[1];
      lastGamma = params[2];

      double[] result;

      if (yearly) {
        lastOmega = params[3];
        System.out.println("Alpha is: " + params[0] + " Beta is: " + params[1] + " Gamma is: " + params[2] +
            " Omega is: " + params[3]);
        result = forecast(y, params[0], params[1], params[2], params[3]);
      } else {
        System.out.println("Alpha is: " + params[0] + " Beta is: " + params[1] + " Gamma is: " + params[2]);
        result = forecast(y, params[0], params[1], params[2], -1);
      }

      resultArray[k] = result[0];
      sseArray[k] = result[1];
      errorArray[k] = result[2];
    }


    resultDF.addSeries(COL_TIME, LongSeries.buildFrom(resultTimeArray));
    resultDF.setIndex(COL_TIME);
    resultDF.addSeries(COL_VALUE, DoubleSeries.buildFrom(resultArray));
    resultDF.addSeries(COL_ERROR, DoubleSeries.buildFrom(errorArray));
    resultDF.addSeries(COL_SSE, DoubleSeries.buildFrom(sseArray));
    return resultDF;
  }

  private static double calculateLowerUpperConfidenceBoundary(List<Double> givenNumbers, double zscore) {

    // calculate the mean value (= average)
    double sum = 0.0;
    for (double num : givenNumbers) {
      sum += num;
    }
    double mean = sum / givenNumbers.size();

    // calculate standard deviation
    double squaredDifferenceSum = 0.0;
    for (double num : givenNumbers) {
      squaredDifferenceSum += (num - mean) * (num - mean);
    }
    double variance = squaredDifferenceSum / givenNumbers.size();
    double standardDeviation = Math.sqrt(variance);

    // value for 95% confidence interval, source: https://en.wikipedia.org/wiki/Confidence_interval#Basic_Steps
    return zscore * standardDeviation;
  }

  private DataFrame getDailyDFFromSubDailyPoint(DataFrame originalDF, Long time, int lookback) {
    LongSeries longSeries = (LongSeries) originalDF.get(COL_TIME);
    DateTime dt = new DateTime(time);
    DataFrame df = DataFrame.builder(COL_TIME, COL_VALUE).build();

    for (int i = 0; i < lookback; i++) {
      DateTime subDt = new DateTime(dt);
      subDt = subDt.minusDays(1);
      long t = subDt.getMillis();

      int index = longSeries.find(t);
      int backtrackCounter = 0;
      if (index != -1) {
        df = df.append(originalDF.slice(index, index+1));
      } else { // If the 1 day look back data doesn't exist, use wow data
        while (index == -1) {
          subDt = subDt.minusDays(period);
          long tWO1W = subDt.getMillis();
          index = longSeries.find(tWO1W);
          if (index != -1) {
            df = df.append(originalDF.slice(index, index+1));
          }
          if (backtrackCounter++ > 4) { // search up to 4 weeks, else plugin the last value
            double lastVal = (originalDF.get(COL_VALUE)).getDouble(longSeries.find(time));
            DateTime lastDt = dt.minusDays(1);
            DataFrame append = DataFrame.builder(COL_TIME, COL_VALUE).append(new Object[]{lastDt, lastVal}).build();
            df = df.append(append);
            break;
          }
        }
      }
      dt = dt.minusDays(1);
    }
    df = df.reverse();
    return df;
  }

  private double[] fitModelWithBOBYQA(double[] y, double lastAlpha, double lastBeta, double lastGamma,
      double lastOmega) {
    BOBYQAOptimizer optimizer = new BOBYQAOptimizer(7);
    if (lastAlpha < 0) {
      lastAlpha = 0.1;
    }
    if (lastBeta < 0) {
      lastBeta = 0.01;
    }
    if (lastGamma < 0) {
      lastGamma = 0.001;
    }
    InitialGuess initGuess;
    MaxIter maxIter;
    MaxEval maxEval;
    GoalType goal;
    ObjectiveFunction objectiveFunction;
    SimpleBounds bounds;
    maxIter = new MaxIter(30000);
    maxEval = new MaxEval(30000);
    goal = GoalType.MINIMIZE;

    if (yearly) {
      objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
        public double value(double[] params) {
          return forecast(y, params[0], params[1], params[2], params[3])[1];
        }
      });
      if (lastOmega < 0) {
        lastOmega = 0.001;
      }
      initGuess = new InitialGuess(new double[]{lastAlpha, lastBeta, lastGamma, lastOmega});
      bounds = new SimpleBounds(new double[]{0.001, 0.001, 0.001, 0.001}, new double[]{0.999, 0.999, 0.999, 0.999});

    } else {
      objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
        public double value(double[] params) {
          return forecast(y, params[0], params[1], params[2], -1)[1];
        }
      });

      initGuess = new InitialGuess(new double[]{lastAlpha, lastBeta, lastGamma});
      bounds = new SimpleBounds(new double[]{0.001, 0.001, 0.001}, new double[]{0.999, 0.999, 0.999});
    }

    double[] params;
    try {
      PointValuePair optimal = optimizer.optimize(objectiveFunction, goal, bounds, initGuess, maxIter, maxEval);
      params = optimal.getPoint();
    } catch (Exception e) {
      System.out.println(e);
      params = new double[3];
      params[0] = lastAlpha;
      params[1] = lastBeta;
      params[2] = lastGamma;
      if (yearly) {
        params = new double[4];
        params[0] = lastAlpha;
        params[1] = lastBeta;
        params[2] = lastGamma;
        params[3] = lastOmega;
      }
    }
    return params;
  }

  private static double pValueToZscore(double p) {
    double z = Math.sqrt(2) * Erf.erfcInv(2*p);
    return(z);
  }
}
