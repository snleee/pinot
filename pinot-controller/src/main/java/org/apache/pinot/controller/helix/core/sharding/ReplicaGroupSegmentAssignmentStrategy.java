/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignment;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to represent the segment assignment strategy based on the concept of a replica group.
 *
 * A replica group is a pool of servers that is guaranteed to have all segments for a partition or a table
 * (depend on the configuration). If the broker is aware of the replica group, the broker can prune servers
 * when scattering and gathering requests because queries can be answered with a subset of servers.
 *
 */
public class ReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupSegmentAssignmentStrategy.class);
  private static final Random RANDOM = new Random();

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {

    // Fetch the partition mapping table from the property store.
    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(propertyStore);
    ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment =
        partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    // Fetch the segment assignment related configurations.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    boolean mirrorAssignmentAcrossReplicaGroups = replicaGroupStrategyConfig.getMirrorAssignmentAcrossReplicaGroups();
    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();

    // Read partition number for a segment
    int partitionNumber = 0;
    if (partitionColumn != null) {
      // TODO: support multiple partitions
      partitionNumber =
          ((SegmentMetadataImpl) segmentMetadata).getColumnMetadataFor(partitionColumn).getPartitions().iterator()
              .next();
    }
    return getAssignedInstancesHelper(propertyStore, tableNameWithType, segmentMetadata.getName(), numReplicas,
        partitionNumber, mirrorAssignmentAcrossReplicaGroups);
  }

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int numReplicas, String tenantName) {
    // Fetch replica group strategy config
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    boolean mirrorAssignmentAcrossReplicaGroups = replicaGroupStrategyConfig.getMirrorAssignmentAcrossReplicaGroups();
    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();

    // Read partition number for a segment
    int partitionNumber = 0;
    if (partitionColumn != null) {
      partitionNumber =
          segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(partitionColumn).getPartitions()
              .iterator().next();
    }
    return getAssignedInstancesHelper(propertyStore, tableNameWithType, segmentZKMetadata.getSegmentName(), numReplicas,
        partitionNumber, mirrorAssignmentAcrossReplicaGroups);
  }

  private List<String> getAssignedInstancesHelper(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType, String segmentName, int numReplicas, int partitionNumber,
      boolean mirrorAssignmentAcrossReplicaGroups) {
    // Fetch the partition mapping table from the property store.
    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(propertyStore);
    ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment =
        partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    // Perform the segment assignment.
    // If mirror assignment is on, we randomly pick the index and use the same index for all replica groups.
    // Else, we randomly pick server from each replica group.
    List<String> selectedInstanceList = new ArrayList<>();
    int index = 0;
    for (int groupId = 0; groupId < numReplicas; groupId++) {
      List<String> instancesInReplicaGroup =
          replicaGroupPartitionAssignment.getInstancesFromReplicaGroup(partitionNumber, groupId);
      int numInstances = instancesInReplicaGroup.size();
      if (mirrorAssignmentAcrossReplicaGroups) {
        // Randomly pick the index and use the same index for all replica groups.
        if (groupId == 0) {
          index = RANDOM.nextInt(numInstances);
        }
      } else {
        // Randomly pick the index for all replica groups.
        index = RANDOM.nextInt(numInstances);
      }
      selectedInstanceList.add(instancesInReplicaGroup.get(index));
    }

    LOGGER.info("Segment assignment result for : " + segmentName + ", in resource : " + tableNameWithType
        + ", selected instances: " + Arrays.toString(selectedInstanceList.toArray()));

    return selectedInstanceList;
  }
}
