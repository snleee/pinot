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

import java.util.List;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RollupConfig {

  @ConfigKey("rollupType")
  String rollupType;

  @ConfigKey("preAggregateType")
  @UseChildKeyHandler(SimpleMapChildKeyHandler.class)
  Map<String, String> _preAggregateType;

  @ConfigKey("multiLevelRollupSettings")
  List<MultiLevelRollupSetting> _multiLevelRollupSettings;

  public String getRollupType() {
    return rollupType;
  }

  public void setRollupType(String rollupType) {
    this.rollupType = rollupType;
  }

  public Map<String, String> getPreAggregateType() {
    return _preAggregateType;
  }

  public void setPreAggregateType(Map<String, String> preAggregateType) {
    _preAggregateType = preAggregateType;
  }

  public List<MultiLevelRollupSetting> getMultiLevelRollupSettings() {
    return _multiLevelRollupSettings;
  }

  public void setMultiLevelRollupSettings(List<MultiLevelRollupSetting> multiLevelRollupSettings) {
    _multiLevelRollupSettings = multiLevelRollupSettings;
  }

}
