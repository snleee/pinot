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

import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentMergeConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(RollupConfig.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String MERGE_TYPE = "mergeType";
  private static final String MERGE_STRATEGY = "mergeStrategy";
  private static final String MIN_NUM_SEGMENTS = "minNumSegments";
  private static final String MIN_NUM_TOTAL_DOCS = "minNumTotalDocs";
  private static final String ROLLUP_CONFIG = "rollupConfig";
  private static final String OLD_SEGMENT_CLEANUP = "oldSegmentCleanup";

  @ConfigKey(value = "mergeType")
  private String _mergeType;

  @ConfigKey(value = "mergeStrategy")
  private String _mergeStrategy;

  @ConfigKey(value = "rollupConfig")
  private RollupConfig _rollupConfig;

  @ConfigKey(value = "minNumSegments")
  private int _minNumSegments;

  @ConfigKey(value = "minNumTotalDocs")
  private long _minNumTotalDocs;

  @ConfigKey(value = "oldSegmentCleanup")
  private boolean _oldSegmentCleanup;

  public String getMergeType() {
    return _mergeType;
  }

  public void setMergeType(String mergeType) {
    _mergeType = mergeType;
  }

  public String getMergeStrategy() {
    return _mergeStrategy;
  }

  public void setMergeStrategy(String mergeStrategy) {
    _mergeStrategy = mergeStrategy;
  }

  public int getMinNumSegments() {
    return _minNumSegments;
  }

  public void setMinNumSegments(int minNumSegments) {
    _minNumSegments = minNumSegments;
  }

  public long getMinNumTotalDocs() {
    return _minNumTotalDocs;
  }

  public void setMinNumTotalDocs(long minNumTotalDocs) {
    _minNumTotalDocs = minNumTotalDocs;
  }

  public boolean getOldSegmentCleanup() {
    return _oldSegmentCleanup;
  }

  public void setOldSegmentCleanup(boolean oldSegmentCleanup) {
    _oldSegmentCleanup = oldSegmentCleanup;
  }

  public RollupConfig getRollupConfig() {
    return _rollupConfig;
  }

  public void setRollupConfig(RollupConfig rollupConfig) {
    _rollupConfig = rollupConfig;
  }

  @Nonnull
  public JSONObject toJSON() {
    JSONObject mergeRollupConfigJsonObject = new JSONObject();
    try {
      mergeRollupConfigJsonObject.put(MERGE_TYPE, _mergeType);
      mergeRollupConfigJsonObject.put(MERGE_STRATEGY, _mergeStrategy);
      mergeRollupConfigJsonObject.put(ROLLUP_CONFIG, _rollupConfig);
      mergeRollupConfigJsonObject.put(MIN_NUM_SEGMENTS, _minNumSegments);
      mergeRollupConfigJsonObject.put(MIN_NUM_TOTAL_DOCS, _minNumTotalDocs);
      mergeRollupConfigJsonObject.put(OLD_SEGMENT_CLEANUP, _oldSegmentCleanup);
    } catch (Exception e) {
      LOGGER.error("Failed to convert rollup config to json", e);
    }

    return mergeRollupConfigJsonObject;
  }

  public String toJSONString() {
    try {
      return toJSON().toString(2);
    } catch (Exception e) {
      return e.toString();
    }
  }

  public static void main(String[] args) {
    SegmentMergeConfig config = new SegmentMergeConfig();
    config.setMergeType("CONCATENATE");
    System.out.println(config.toJSONString());
    System.out.println(config.toJSON());
  }
}
