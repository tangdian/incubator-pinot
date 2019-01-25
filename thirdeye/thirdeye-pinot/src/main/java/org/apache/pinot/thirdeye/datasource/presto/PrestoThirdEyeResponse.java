/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource.presto;

import java.util.List;
import java.util.Map;

import org.apache.pinot.thirdeye.api.TimeSpec;
import org.apache.pinot.thirdeye.datasource.BaseThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponseRow;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeResponse;


public class PrestoThirdEyeResponse extends BaseThirdEyeResponse{
  private PinotThirdEyeResponse delegate;

  public PrestoThirdEyeResponse(ThirdEyeRequest request, List<String[]> rows, TimeSpec dataTimeSpec) {
    super(request, dataTimeSpec);
    delegate = new PinotThirdEyeResponse(request, rows, dataTimeSpec);
  }

  @Override
  public int getNumRowsFor(MetricFunction metricFunction) {
    return delegate.getNumRowsFor(metricFunction);
  }

  @Override
  public Map<String, String> getRow(MetricFunction metricFunction, int rowId) {
    return delegate.getRow(metricFunction, rowId);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public int getNumRows() {
    return delegate.getNumRows();
  }

  @Override
  public ThirdEyeResponseRow getRow(int rowId) {
    return delegate.getRow(rowId);
  }
}

