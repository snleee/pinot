package org.apache.pinot.plugin.minion.tasks.industryv2;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndustryV2MigrationTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndustryV2MigrationTaskExecutor.class);
  public static final String INDUSTRY_V2_MIGRATION_TASK_TYPE = "IndustryV2Migration";

  private static final String INDUSTRY_V1_COLUMN_NAME_KEY = "industryV1ColumnName";
  private static final String INDUSTRY_V2_COLUMN_NAME_KEY = "industryV2ColumnName";

  @Override
  public SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String industryV1Column = configs.get(INDUSTRY_V1_COLUMN_NAME_KEY);
    String industryV2Column = configs.get(INDUSTRY_V2_COLUMN_NAME_KEY);

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);

    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    // Add the validation
    // 1. v1, v2 column exist in the schema
    // 2. check both v1, v2 column is "int" column
    // TODO: add support for all data types
    Preconditions.checkNotNull(schema.getFieldSpecFor(industryV1Column), "industry v1 column must exist in the schema");
    Preconditions.checkNotNull(schema.getFieldSpecFor(industryV2Column), "industry v2 column must exist in the schema");
    Preconditions.checkState(schema.getFieldSpecFor(industryV1Column).getDataType() == FieldSpec.DataType.INT, "industry v1 column must be INT column");
    Preconditions.checkState(schema.getFieldSpecFor(industryV1Column).getDataType() == FieldSpec.DataType.INT, "industry v2 column must be INT column");


//    TableConfig tableConfig =
//        JsonUtils.fileToObject(new File("/Users/snlee/data/ContentGesture/table.config"), TableConfig.class);
//    Schema schema =
//        JsonUtils.fileToObject(new File("/Users/snlee/data/ContentGesture/schema"), Schema.class);

    // Create the record reader from the input segment
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Start IndustryV2 conversion for table: {}, segment: {}", tableConfig.getTableName(), segmentName);

    try (IndustryV2RecordReader industryV2RecordReader = new IndustryV2RecordReader(industryV1Column, industryV2Column)) {
      industryV2RecordReader.init(indexDir, null, null);

      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(workingDir.getPath());
      config.setSegmentName(segmentName);

      // Keep index creation time the same as original segment because both segments use the same raw data.
      // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
      // identify if the new pushed segment has newer data than the existing one.
      config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

      // The time column type info is not stored in the segment metadata.
      // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
      if (segmentMetadata.getTimeInterval() != null) {
        config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
        config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
        config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
        config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
      }

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, industryV2RecordReader);
      driver.build();
    }
    return new SegmentConversionResult.Builder().setSegmentName(segmentName)
        .setFile(new File(workingDir.getPath(), segmentName)).setTableNameWithType(tableNameWithType).build();
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections
        .singletonMap(INDUSTRY_V2_MIGRATION_TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
