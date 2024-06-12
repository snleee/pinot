package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.plugin.minion.tasks.industryv2.IndustryV2MigrationTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.industryv2.IndustryV2MigrationTaskGenerator;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IndustryV2MigrationMinionClusterIntegrationTest extends BaseClusterIntegrationTest{
  private static final String TEST_TABLE = "myTable1";
  private static final String INDUSTRY_V2_MIGRATION_TASK_TYPE = "IndustryV2Migration";

  protected final File _segmentDir = new File(_tempDir, "segmentDir1");
  protected final File _tarDir = new File(_tempDir, "tarDir1");

  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;


  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig =
        createOfflineTableConfig(TEST_TABLE, getIndustryV2MigrationTaskConfig(), null);
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(TEST_TABLE, _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Register task executor as well
    startMinion();

    // Register task generator to the controller
    PinotTaskGenerator taskGenerator = new IndustryV2MigrationTaskGenerator();
    taskGenerator.init(_controllerStarter.getTaskManager().getClusterInfoAccessor());
    _controllerStarter.getTaskManager().getTaskGeneratorRegistry()
        .registerTaskGenerator(taskGenerator);

    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();

  }

  @Test
  public void testSingleLevelConcat()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE);
    int numTasks = 0;

    Schema schema = getSchema();
    FieldSpec arrTimeV2 = new DimensionFieldSpec("ArrTimeV2", FieldSpec.DataType.INT, true);
    schema.addField(arrTimeV2);
    addSchema(schema);

    String tasks = _taskManager.scheduleTasks(offlineTableName).get(INDUSTRY_V2_MIGRATION_TASK_TYPE);
    System.out.println("Invoke task schedule");
    waitForTaskToComplete();

    Thread.sleep(1000000000L);
  }

  protected void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(INDUSTRY_V2_MIGRATION_TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }

  private TableTaskConfig getIndustryV2MigrationTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("industryV1ColumnName", "ArrTime");
    tableTaskConfigs.put("industryV2ColumnName", "ArrTimeV2");
    return new TableTaskConfig(Collections.singletonMap("IndustryV2Migration", tableTaskConfigs));
  }

  private TableConfig createOfflineTableConfig(String tableName, TableTaskConfig taskConfig,
      @Nullable SegmentPartitionConfig partitionConfig) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(taskConfig).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled()).setSegmentPartitionConfig(partitionConfig).build();
  }

  @Override
  protected void startMinion()
      throws Exception {
    FileUtils.deleteQuietly(new File(CommonConstants.Minion.DEFAULT_INSTANCE_BASE_DIR));
    PinotConfiguration minionConf = getDefaultMinionConfiguration();
    minionConf.setProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    minionConf.setProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    _minionStarter = new MinionStarter();
    _minionStarter.init(minionConf);
    // Register task executor to the minion
    _minionStarter.registerTaskExecutorFactory(new IndustryV2MigrationTaskExecutorFactory());
    _minionStarter.start();
  }
}
