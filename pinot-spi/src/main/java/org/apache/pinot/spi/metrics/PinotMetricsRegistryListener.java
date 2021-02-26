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
package org.apache.pinot.spi.metrics;

/**
 * Listeners for events from the registry.  Listeners must be thread-safe.
 */
public interface PinotMetricsRegistryListener {
  /**
   * Called when a metric has been added to the {@link PinotMetricsRegistry}.
   *
   * @param name   the name of the {@link PinotMetric}
   * @param metric the {@link PinotMetric}
   */
  void onMetricAdded(PinotMetricName name, PinotMetric metric);

  /**
   * Called when a metric has been removed from the {@link PinotMetricsRegistry}.
   *
   * @param name the name of the {@link PinotMetric}
   */
  void onMetricRemoved(PinotMetricName name);

  /**
   * Returned the actual object of MetricsRegistryListener.
   */
  Object getMetricsRegistryListener();
}
