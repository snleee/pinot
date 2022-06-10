package org.apache.pinot.plugin.minion.tasks.industryv2;

import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;


public class IndustryV2MigrationTaskExecutorFactory implements PinotTaskExecutorFactory {
  public static final String INDUSTRY_V2_MIGRATION_TASK_TYPE = "IndustryV2Migration";

  @Override
  public void init(MinionTaskZkMetadataManager minionTaskZkMetadataManager) {
  }

  @Override
  public void init(MinionTaskZkMetadataManager minionTaskZkMetadataManager, MinionConf minionConf) {
  }

  @Override
  public String getTaskType() {
    return INDUSTRY_V2_MIGRATION_TASK_TYPE;
  }

  @Override
  public PinotTaskExecutor create() {
    return new IndustryV2MigrationTaskExecutor();
  }
}
