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
package com.linkedin.pinot.broker.routing;


import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;


public class RoutingTableBenchmark {
  public static final int NUM_ROUNDS = 10000;

//  public static void main(String[] args) throws Exception {
////    URL resourceUrl = RoutingTableBenchmark.class.getClassLoader().getResource("SampleExternalView.json");
////    URL resourceUrl = RoutingTableBenchmark.class.getClassLoader().getResource("SampleRealtimeExternalView.json");
//    URL resourceUrl = RoutingTableBenchmark.class.getClassLoader().getResource("AdStatistics.json");
//
//    Assert.assertNotNull(resourceUrl);
//    String fileName = resourceUrl.getFile();
//
//    byte[] externalViewBytes = IOUtils.toByteArray(new FileInputStream(fileName));
//    ExternalView externalView = new ExternalView((ZNRecord) new ZNRecordSerializer().deserialize(externalViewBytes));
//    String tableName = externalView.getResourceName();
//    List<InstanceConfig> instanceConfigs = getInstanceConfigs(externalView);
//
//    TableConfig preComputeTableConfig = generatePreComputeTableConfig(tableName);
//    TableConfig dynamicTableConfig = generateDynamicTableConfig(tableName);
//
//    HelixExternalViewBasedRouting precomputeRouting = new HelixExternalViewBasedRouting(null, null, new BaseConfiguration());
//    precomputeRouting.markDataResourceOnline(preComputeTableConfig, externalView, instanceConfigs);
//
//    HelixExternalViewBasedRouting dynamicRouting = new HelixExternalViewBasedRouting(null, null, new BaseConfiguration());
//    dynamicRouting.markDataResourceOnline(dynamicTableConfig, externalView, instanceConfigs);
//
////    System.out.println("Pre-compute Routing table builder");
////    for (int i = 0; i < NUM_ROUNDS; i++) {
////      long start = System.nanoTime();
////      Map<String, List<String>> routingTable = precomputeRouting.getRoutingTable(new RoutingTableLookupRequest(tableName));
////      long end = System.nanoTime();
////      System.out.println("round " + i + ": " + (end - start) / 1000000.0 + " ms");
////    }
////    System.out.println("");
//
//    System.out.println("Dynamic Routing table builder");
//    for (int i = 0; i < NUM_ROUNDS; i++) {
//      long start = System.nanoTime();
//      Map<String, List<String>> routingTable = dynamicRouting.getRoutingTable(new RoutingTableLookupRequest(tableName));
//      long end = System.nanoTime();
//      System.out.println("round " + i + ": " + (end - start) / 1000000.0 + " ms");
//    }
//  }
//
//  private static List<InstanceConfig> getInstanceConfigs(ExternalView externalView) {
//    List<InstanceConfig> instanceConfigs = new ArrayList<>();
//    Set<String> instances = new HashSet<>();
//
//    // Collect all unique instances
//    for (String partitionName : externalView.getPartitionSet()) {
//      for (String instance : externalView.getStateMap(partitionName).keySet()) {
//        if (!instances.contains(instance)) {
//          instanceConfigs.add(new InstanceConfig(instance));
//          instances.add(instance);
//        }
//      }
//    }
//
//    return instanceConfigs;
//  }
//
//  private static TableConfig generatePreComputeTableConfig(String tableName) throws Exception {
//    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
//    TableConfig.Builder builder = new TableConfig.Builder(tableType);
//    builder.setTableName(tableName);
//    TableConfig tableConfig = builder.build();
////    tableConfig.getRoutingConfig().setRoutingTableBuilderName("KafkaLowLevel");
//    return tableConfig;
//  }
//
//  private static TableConfig generateDynamicTableConfig(String tableName) throws Exception {
//    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
//    TableConfig.Builder builder = new TableConfig.Builder(tableType);
//    builder.setTableName(tableName);
//    TableConfig tableConfig = builder.build();
//    Map<String, String> option = new HashMap<>();
//    option.put(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY, "true");
//    tableConfig.getRoutingConfig().setRoutingTableBuilderOptions(option);
////    tableConfig.getRoutingConfig().setRoutingTableBuilderName("KafkaLowLevel");
//    return tableConfig;
//  }

}

