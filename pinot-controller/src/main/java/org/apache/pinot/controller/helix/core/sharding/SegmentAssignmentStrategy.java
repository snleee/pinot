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

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.segment.SegmentMetadata;


/**
 * Given a segmentMetadata, each strategy has to implement its own method to compute the assigned instances.
 *
 *
 */
public interface SegmentAssignmentStrategy {

  List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentMetadata segmentMetadata, int numReplicas, String tenantName);

  List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int numReplicas, String tenantName);
}


