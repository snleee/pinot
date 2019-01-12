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
package com.linkedin.pinot.common.config;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiLevelRollupSetting {
  @ConfigKey("timeInputFormat")
  public String _timeInputFormat;

  @ConfigKey("timeOutputFormat")
  public String _timeOutputFormat;

  @ConfigKey("timeOutputGranularity")
  public String _timeOutputGranularity;

  @ConfigKey("minNumSegments")
  public int _minNumSegments;

  @ConfigKey("minNumTotalDocs")
  public int _minNumTotalDocs;

  public String getTimeInputFormat() {
    return _timeInputFormat;
  }

  public void setTimeInputFormat(String timeInputFormat) {
    _timeInputFormat = timeInputFormat;
  }

  public String getTimeOutputFormat() {
    return _timeOutputFormat;
  }

  public void setTimeOutputFormat(String timeOutputFormat) {
    _timeOutputFormat = timeOutputFormat;
  }

  public String getTimeOutputGranularity() {
    return _timeOutputGranularity;
  }

  public void setTimeOutputGranularity(String timeOutputGranularity) {
    _timeOutputGranularity = timeOutputGranularity;
  }

  public int getMinNumSegments() {
    return _minNumSegments;
  }

  public void setMinNumSegments(int minNumSegments) {
    _minNumSegments = minNumSegments;
  }

  public int getMinNumTotalDocs() {
    return _minNumTotalDocs;
  }

  public void setMinNumTotalDocs(int minNumTotalDocs) {
    _minNumTotalDocs = minNumTotalDocs;
  }
}
