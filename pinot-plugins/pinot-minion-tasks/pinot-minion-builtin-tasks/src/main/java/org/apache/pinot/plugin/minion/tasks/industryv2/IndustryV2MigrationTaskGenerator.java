package org.apache.pinot.plugin.minion.tasks.industryv2;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;


public class IndustryV2MigrationTaskGenerator extends BaseTaskGenerator {
  public static final String INDUSTRY_V2_MIGRATION_TASK_TYPE = "IndustryV2Migration";
  private static final String INDUSTRY_V1_COLUMN_NAME_KEY = "industryV1ColumnName";
  private static final String INDUSTRY_V2_COLUMN_NAME_KEY = "industryV2ColumnName";

  private static final int TABLE_MAX_NUM_TASKS = 100;

  @Override
  public String getTaskType() {
    return INDUSTRY_V2_MIGRATION_TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    LOGGER.info("Start generating IndustryV2MigrationTask");
    String taskType = getTaskType();
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {

      String tableName = tableConfig.getTableName();
      if (tableConfig.getTableType() == TableType.REALTIME) {
        LOGGER.warn("Skip generating task: {} for real-time table: {}", taskType, tableName);
        continue;
      }

      Map<String, String> taskConfigs;
      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      if (tableTaskConfig == null) {
        LOGGER.warn("Failed to find task config for table: {}", tableName);
        continue;
      }
      taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", tableName);

      String industryV1Column = taskConfigs.get(INDUSTRY_V1_COLUMN_NAME_KEY);
      String industryV2Column = taskConfigs.get(INDUSTRY_V2_COLUMN_NAME_KEY);

      LOGGER.info("Start generating task configs for table: {} for task: {}, industryV1: {}, industryV2: {}", tableName,
          taskType, industryV1Column, industryV2Column);
      // Get max number of tasks for this table
      int tableMaxNumTasks;
      String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
      if (tableMaxNumTasksConfig != null) {
        try {
          tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
        } catch (Exception e) {
          tableMaxNumTasks = TABLE_MAX_NUM_TASKS;
          LOGGER.warn("MaxNumTasks have been wrongly set for table : {}, and task {}", tableName, taskType);
        }
      } else {
        tableMaxNumTasks = TABLE_MAX_NUM_TASKS;
      }
      List<SegmentZKMetadata> offlineSegmentsZKMetadata = _clusterInfoAccessor.getSegmentsZKMetadata(tableName);
      List<SegmentZKMetadata> processedSegmentZKMetadata = new ArrayList<>();
      List<SegmentZKMetadata> notProcessedSegmentZKMetadata = new ArrayList<>();

      for (SegmentZKMetadata segmentMetadata: offlineSegmentsZKMetadata) {

        if (segmentMetadata.getCustomMap() != null && segmentMetadata.getCustomMap().containsKey(
            INDUSTRY_V2_MIGRATION_TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) {
          processedSegmentZKMetadata.add(segmentMetadata);
        } else {
          notProcessedSegmentZKMetadata.add(segmentMetadata);
        }
      }

      int tableNumTasks = 0;
      Set<Segment> runningSegments =
          TaskGeneratorUtils.getRunningSegments(INDUSTRY_V2_MIGRATION_TASK_TYPE, _clusterInfoAccessor);
      for (SegmentZKMetadata segmentZKMetadata : notProcessedSegmentZKMetadata) {
        Map<String, String> configs = new HashMap<>();
        String segmentName = segmentZKMetadata.getSegmentName();

        //skip running segment
        if (runningSegments.contains(new Segment(tableName, segmentName))) {
          continue;
        }
        if (tableNumTasks == tableMaxNumTasks) {
          break;
        }
        configs.put(MinionConstants.TABLE_NAME_KEY, tableName);
        configs.put(MinionConstants.SEGMENT_NAME_KEY, segmentName);
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, segmentZKMetadata.getDownloadUrl());
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segmentZKMetadata.getCrc()));
        configs.put(INDUSTRY_V1_COLUMN_NAME_KEY, industryV1Column);
        configs.put(INDUSTRY_V2_COLUMN_NAME_KEY, industryV2Column);
        pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
        tableNumTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {} " + "for task: {}", tableNumTasks, tableName,
          taskType);
    }
    return pinotTaskConfigs;
  }
}
