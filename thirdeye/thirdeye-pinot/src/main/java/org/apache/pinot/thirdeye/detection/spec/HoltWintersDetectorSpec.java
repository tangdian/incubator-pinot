package org.apache.pinot.thirdeye.detection.spec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;


@JsonIgnoreProperties(ignoreUnknown = true)
public class HoltWintersDetectorSpec  extends AbstractSpec  {


  private double alpha = -1;
  private double beta = -1;
  private double gamma = -1;

  private int period = 7;

  private double pvalue = 0.05;
  private int lookback = 90;

  private double intervalWidth = 1;
  private String pattern = "UP_OR_DOWN";

  private String method = "multiplicative";
  private String yearly = "false";



  public void setYearly(String yearly) {
    this.yearly = yearly;
  }





  private String monitoringGranularity = MetricSlice.NATIVE_GRANULARITY.toAggregationGranularityString(); // use native granularity by default


  public String getPattern() {
    return pattern;
  }

  public double getPvalue() {
    return pvalue;
  }

  public double getAlpha() {
    return alpha;
  }

  public double getBeta() {
    return beta;
  }

  public double getGamma() {
    return gamma;
  }

  public int getPeriod() {
    return period;
  }

  public int getLookback() {
    return lookback;
  }

  public String getMethod() {
    return method;
  }

  public String getMonitoringGranularity() {
    return monitoringGranularity;
  }

  public double getIntervalWidth() {
    return intervalWidth;
  }

  public String getYearly() {
    return yearly;
  }

  public void setIntervalWidth(double intervalWidth) {
    this.intervalWidth = intervalWidth;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public void setBeta(double beta) {
    this.beta = beta;
  }

  public void setGamma(double gamma) {
    this.gamma = gamma;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  public void setLookback(int lookback) {
    this.lookback = lookback;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public void setPvalue(double pvalue) {
    this.pvalue = pvalue;
  }

  public void setMethod(String method) {
    this.method = method;
  }
}
