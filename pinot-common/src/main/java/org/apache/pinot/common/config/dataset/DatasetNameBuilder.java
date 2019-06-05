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
package org.apache.pinot.common.config.dataset;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.SegmentName;


public class DatasetNameBuilder {
  public static final DatasetNameBuilder OFFLINE = new DatasetNameBuilder(CommonConstants.Helix.DatasetType.OFFLINE);
  public static final DatasetNameBuilder REALTIME = new DatasetNameBuilder(CommonConstants.Helix.DatasetType.REALTIME);

  private static final String TYPE_SUFFIX_SEPARATOR = "_";

  private final String _typeSuffix;

  private DatasetNameBuilder(@Nonnull CommonConstants.Helix.DatasetType datasetType) {
    _typeSuffix = TYPE_SUFFIX_SEPARATOR + datasetType.toString();
  }

  /**
   * Get the dataset name builder for the given dataset type.
   * @param datasetType Dataset type
   * @return Dataset name builder for the given dataset type
   */
  @Nonnull
  public static DatasetNameBuilder forType(@Nonnull CommonConstants.Helix.DatasetType datasetType) {
    if (datasetType == CommonConstants.Helix.DatasetType.OFFLINE) {
      return OFFLINE;
    } else {
      return REALTIME;
    }
  }

  /**
   * Get the dataset name builder for the given table type.
   * @param tableType Table type
   * @return Dataset name builder for the given table type
   */
  @Nonnull
  public static DatasetNameBuilder forType(@Nonnull CommonConstants.Helix.TableType tableType) {
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      return OFFLINE;
    } else {
      return REALTIME;
    }
  }

  /**
   * Get the dataset name with type suffix.
   *
   * @param datasetName Dataset name with or without type suffix
   * @return Dataset name with type suffix
   */
  @Nonnull
  public String datasetNameWithType(@Nonnull String datasetName) {
    Preconditions.checkArgument(!datasetName.contains(SegmentName.SEPARATOR),
        "Table name: %s cannot contain two consecutive underscore characters", datasetName);

    if (datasetName.endsWith(_typeSuffix)) {
      return datasetName;
    } else {
      return datasetName + _typeSuffix;
    }
  }

  /**
   * Return Whether the dataset has type suffix that matches the builder type.
   *
   * @param datasetName Dataset name with or without type suffix
   * @return Whether the table has type suffix that matches the builder type
   */
  public boolean datasetHasTypeSuffix(@Nonnull String datasetName) {
    return datasetName.endsWith(_typeSuffix);
  }

  /**
   * Get the dataset type based on the given table name with type suffix.
   *
   * @param datasetName Dataset name with or without type suffix
   * @return Dataset type for the given table name, null if cannot be determined by table name
   */
  @Nullable
  public static CommonConstants.Helix.DatasetType getDatasetTypeFromTableName(@Nonnull String datasetName) {
    if (OFFLINE.datasetHasTypeSuffix(datasetName)) {
      return CommonConstants.Helix.DatasetType.OFFLINE;
    }
    if (REALTIME.datasetHasTypeSuffix(datasetName)) {
      return CommonConstants.Helix.DatasetType.REALTIME;
    }
    return null;
  }

  /**
   * Extract the raw dataset name from the given dataset name with type suffix.
   *
   * @param datasetName Dataset name with or without type suffix
   * @return Dataset name without type suffix
   */
  @Nonnull
  public static String extractRawDatasetName(@Nonnull String datasetName) {
    if (OFFLINE.datasetHasTypeSuffix(datasetName)) {
      return datasetName.substring(0, datasetName.length() - OFFLINE._typeSuffix.length());
    }
    if (REALTIME.datasetHasTypeSuffix(datasetName)) {
      return datasetName.substring(0, datasetName.length() - REALTIME._typeSuffix.length());
    }
    return datasetName;
  }
}
