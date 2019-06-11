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
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Random assign segment to instances.
 *
 *
 */
public class RandomAssignmentStrategy implements SegmentAssignmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(RandomAssignmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType, String helixClusterName,
      SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {
    return getAssignedInstancesHelper(helixManager, tableNameWithType, numReplicas, tenantName,
        segmentMetadata.getName());
  }

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int numReplicas, String tenantName) {
    return getAssignedInstancesHelper(helixManager, tableNameWithType, numReplicas, tenantName,
        segmentZKMetadata.getSegmentName());
  }

  private List<String> getAssignedInstancesHelper(HelixManager helixManager, String tableNameWithType, int numReplicas,
      String tenantName, String segmentName) {
    String serverTenantName = TagNameUtils.getOfflineTagForTenant(tenantName);
    final Random random = new Random(System.currentTimeMillis());

    List<String> allInstanceList = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTenantName);
    List<String> selectedInstanceList = new ArrayList<>();
    for (int i = 0; i < numReplicas; ++i) {
      final int idx = random.nextInt(allInstanceList.size());
      selectedInstanceList.add(allInstanceList.get(idx));
      allInstanceList.remove(idx);
    }
    LOGGER.info("Segment assignment result for : " + segmentName + ", in resource : " + tableNameWithType
        + ", selected instances: " + Arrays.toString(selectedInstanceList.toArray()));

    return selectedInstanceList;
  }
}
