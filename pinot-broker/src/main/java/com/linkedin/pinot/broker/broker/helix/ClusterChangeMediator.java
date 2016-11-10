/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import java.util.List;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;


/**
 * Manages the interactions between Helix cluster changes, the routing table and the connection pool.
 */
public class ClusterChangeMediator implements LiveInstanceChangeListener, ExternalViewChangeListener, InstanceConfigChangeListener {
  private HelixExternalViewBasedRouting _helixExternalViewBasedRouting;

  public ClusterChangeMediator(HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    _helixExternalViewBasedRouting.processExternalViewChange();
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    _helixExternalViewBasedRouting.processInstanceConfigChange();
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
  }
}
