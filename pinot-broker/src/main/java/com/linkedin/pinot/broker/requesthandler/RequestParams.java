/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.requesthandler;

import com.google.common.base.Splitter;
import java.util.Map;
import org.json.JSONObject;

import static com.linkedin.pinot.common.utils.CommonConstants.Broker.Request.*;


public class RequestParams {

  private String _query;
  private Map<String, String> _debugOptions = null;
  private boolean _trace;
  private boolean _validateQuery;

  public RequestParams(JSONObject request) {
    _query = request.getString(PQL);
    _validateQuery = request.has(VALIDATE_QUERY) && request.getBoolean(VALIDATE_QUERY);
    _trace = request.has(TRACE) && request.getBoolean(TRACE);
    if (request.has(DEBUG_OPTIONS)) {
      _debugOptions = Splitter.on(';')
          .omitEmptyStrings()
          .trimResults()
          .withKeyValueSeparator('=')
          .split(request.getString(DEBUG_OPTIONS));
    }
  }

  public String getQuery() {
    return _query;
  }

  public Map<String, String> getDebugOptions() {
    return _debugOptions;
  }

  public boolean isTrace() {
    return _trace;
  }

  public boolean isValidateQuery() {
    return _validateQuery;
  }
}
