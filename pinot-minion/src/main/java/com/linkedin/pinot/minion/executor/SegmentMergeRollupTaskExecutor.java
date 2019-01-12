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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMergeRollupTaskExecutor.class);

  @Override
  protected List<SegmentConversionResult> convert(@Nonnull PinotTaskConfig pinotTaskConfig,
      @Nonnull List<File> originalIndexDirs, @Nonnull File workingDir) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String mergeType = configs.get(MinionConstants.SegmentMergeRollupTask.MERGE_TYPE);
    String mergedSegmentName = configs.get(MinionConstants.SegmentMergeRollupTask.MERGED_SEGEMNT_NAME_KEY);
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);

    MergeRollupSegmentConverter rollupSegmentConverter =
        new MergeRollupSegmentConverter.Builder().setMergeType(mergeType)
            .setTableName(tableNameWithType)
            .setSegmentName(mergedSegmentName)
            .setInputIndexDirs(originalIndexDirs)
            .setWorkingDir(workingDir)
            .build();
    List<File> resultFiles = rollupSegmentConverter.convert();
    List<SegmentConversionResult> results = new ArrayList<>();
    for (File file : resultFiles) {
      String outputSegmentName = file.getName();
      results.add(new SegmentConversionResult.Builder().setFile(file)
          .setSegmentName(outputSegmentName)
          .setTableNameWithType(tableNameWithType)
          .build());
    }
    return results;
  }

  @Override
  protected void preprocessBeforeUpload(@Nonnull PinotTaskConfig pinotTaskConfig,
      List<SegmentConversionResult> conversionResults) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String groupsToCoverStr = configs.get(MinionConstants.SegmentMergeRollupTask.GROUPS_TO_COVER_KEY);

    String vipUrl = configs.get(MinionConstants.CONTROLLER_API_URL);
    List<String> childrenGroupIds = new ArrayList<>();
    for (String groupId : groupsToCoverStr.split(",")) {
      childrenGroupIds.add(groupId.trim());
    }
    //    SegmentConversionUtils.uploadSegmentMergeLineage();

    List<String> segments = new ArrayList<>();

    for (SegmentConversionResult conversionResult : conversionResults) {
      segments.add(conversionResult.getSegmentName());
    }

    LOGGER.info("Updating segment merge lineage metadata. segments: {}, childrenGroupIds: {}", segments,
        childrenGroupIds);
    // list of segments, list of children group ids
    // Need to update merged segment
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);

    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType).toString();

    String requestUrl = vipUrl + "/tables" + "/" + rawTableName + "/segments/lineage?type=" + tableType;
    SegmentConversionUtils.uploadSegmentMergeLineage(configs, requestUrl, childrenGroupIds, segments);

  }

  @Override
  protected List<Header> getHttpHeaders() {
    // Since we update segment merge lineage in "pre-process step", we need to indicate this segment upload is merged segment.
    Header mergedSegmentPushHeader =
        new BasicHeader(FileUploadDownloadClient.CustomHeaders.MERGED_SEGMENT_PUSH, "true");
    return Collections.singletonList(mergedSegmentPushHeader);
  }
}
