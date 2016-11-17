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

package com.linkedin.pinot.routing;

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerTimer;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.routing.builder.BalancedRandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.KafkaLowLevelConsumerRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/*
 * TODO
 * Would be better to not have the external view based routing not aware of the fact that there are HLC and LLC
 * implementations. A better way to do it would be to have a RoutingTable implementation that merges the output
 * of an offline routing table and a realtime routing table, with the realtime routing table being aware of the
 * fact that there is both an hlc and llc one.
 */
public class HelixExternalViewBasedRouting implements RoutingTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedRouting.class);
  private final RoutingTableBuilder _offlineRoutingTableBuilder;
  private final RoutingTableBuilder _realtimeHLCRoutingTableBuilder;
  private final RoutingTableBuilder _realtimeLLCRoutingTableBuilder;

  /*
   * _brokerRoutingTable has entries for offline as well as realtime tables. For the
   * realtime tables it has entries consisting of high-level kafka consumer segments only.
   *
   * _llcBrokerRoutingTable has entries for realtime tables only, and has entries for low-level
   * kafka consumer segments only.
   */
  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, List<ServerToSegmentSetMap>> _llcBrokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();

  private final Map<String, Integer> _lastKnownExternalViewVersionMap = new ConcurrentHashMap<>();
  private final Map<String, Map<String, InstanceConfig>> _lastKnownInstanceConfigsForTable = new ConcurrentHashMap<>();
  private final Map<String, InstanceConfig> _lastKnownInstanceConfigs = new ConcurrentHashMap<>();
  private final Map<String, Set<String>> _tablesForInstance = new ConcurrentHashMap<>();

  private final Random _random = new Random(System.currentTimeMillis());
  private final HelixExternalViewBasedTimeBoundaryService _timeBoundaryService;
  private final RoutingTableSelector _routingTableSelector;
  private final HelixManager _helixManager;
  private static final int INVALID_EXTERNAL_VIEW_VERSION = Integer.MIN_VALUE;

  private BrokerMetrics _brokerMetrics;

  public HelixExternalViewBasedRouting(ZkHelixPropertyStore<ZNRecord> propertyStore,
      RoutingTableSelector routingTableSelector, HelixManager helixManager) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    _offlineRoutingTableBuilder = new BalancedRandomRoutingTableBuilder();
    _realtimeHLCRoutingTableBuilder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    _realtimeLLCRoutingTableBuilder = new KafkaLowLevelConsumerRoutingTableBuilder();
    _routingTableSelector = routingTableSelector;
    _helixManager = helixManager;
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    String tableName = request.getTableName();
    List<ServerToSegmentSetMap> serverToSegmentSetMaps;

    boolean forceLLC = false;
    boolean forceHLC = false;

    for (String routingOption : request.getRoutingOptions()) {
      if (routingOption.equalsIgnoreCase("FORCE_HLC")) {
        forceHLC = true;
      }

      if (routingOption.equalsIgnoreCase("FORCE_LLC")) {
        forceLLC = true;
      }
    }

    if (forceHLC && forceLLC) {
      throw new RuntimeException("Trying to force routing to both HLC and LLC at the same time");
    }

    if (CommonConstants.Helix.TableType.REALTIME.equals(TableNameBuilder.getTableTypeFromTableName(tableName))) {
      if (_brokerRoutingTable.containsKey(tableName) && _brokerRoutingTable.get(tableName).size() != 0) {
        if (_llcBrokerRoutingTable.containsKey(tableName) && _llcBrokerRoutingTable.get(tableName).size() != 0) {
          // Has both high and low-level segments. Follow what the routing table selector says.
          if (!forceHLC && (_routingTableSelector.shouldUseLLCRouting(tableName) || forceLLC)) {
            serverToSegmentSetMaps = routeToLLC(tableName);
          } else {
            serverToSegmentSetMaps = routeToHLC(tableName);
          }
        } else {
          // Has only hi-level consumer segments.
          if (forceLLC) {
            throw new RuntimeException("Failed to route to LLC, table has only HLC segments");
          }
          serverToSegmentSetMaps = routeToHLC(tableName);
        }
      } else {
        // May have only low-level consumer segments
        if (forceHLC) {
          throw new RuntimeException("Failed to route to HLC, table has only LLC segments");
        }
        serverToSegmentSetMaps = routeToLLC(tableName);
      }
    } else {  // Offline table, use the conventional routing table
      serverToSegmentSetMaps = _brokerRoutingTable.get(tableName);
    }

    // This map can be potentially empty, for example for realtime table with no segments.
    if (serverToSegmentSetMaps == null || serverToSegmentSetMaps.isEmpty()) {
      return Collections.emptyMap();
    }
    return serverToSegmentSetMaps.get(_random.nextInt(serverToSegmentSetMaps.size())).getRouting();
  }

  @Override
  public boolean routingTableExists(String tableName) {
    return (_brokerRoutingTable.containsKey(tableName) && !_brokerRoutingTable.get(tableName).isEmpty()) || (
        _llcBrokerRoutingTable.containsKey(tableName) && !_llcBrokerRoutingTable.get(tableName).isEmpty());
  }

  private List<ServerToSegmentSetMap> routeToLLC(String tableName) {
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.LLC_QUERY_COUNT, 1);
    }
    return _llcBrokerRoutingTable.get(tableName);
  }

  private List<ServerToSegmentSetMap> routeToHLC(String tableName) {
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.HLC_QUERY_COUNT, 1);
    }
    return _brokerRoutingTable.get(tableName);
  }

  public void setBrokerMetrics(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void start() {
    LOGGER.info("Starting HelixExternalViewBasedRouting!");
  }

  @Override
  public void shutdown() {
    LOGGER.info("Shutting down HelixExternalViewBasedRouting!");
  }

  public void markDataResourceOnline(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
    if (externalView == null) {
      // It is possible for us to get a request to serve a table for which there is no external view. In this case, just
      // keep a bogus last seen external view version to force a rebuild the next time we see an external view.
      _lastKnownExternalViewVersionMap.put(tableName, INVALID_EXTERNAL_VIEW_VERSION);
      return;
    }

    // Build the routing table
    buildRoutingTable(tableName, externalView, instanceConfigList);
  }

  private boolean isRoutingTableRebuildRequired(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    // In unit tests, always rebuild the routing table
    if (_helixManager == null) {
      return true;
    }

    // Do we know about this table?
    if (!_lastKnownExternalViewVersionMap.containsKey(tableName)) {
      LOGGER.info("Routing table for table {} requires rebuild due to it being newly added", tableName);
      return true;
    }

    // Check if the znode version changed
    int externalViewRecordVersion = externalView.getRecord().getVersion();
    int lastKnownExternalViewVersion = _lastKnownExternalViewVersionMap.get(tableName);

    if (externalViewRecordVersion != lastKnownExternalViewVersion || lastKnownExternalViewVersion == INVALID_EXTERNAL_VIEW_VERSION) {
      LOGGER.info(
          "Routing table for table {} requires rebuild due to external view change (current version {}, last known version {})",
          tableName, externalViewRecordVersion, lastKnownExternalViewVersion);
      return true;
    }

    // Check if there are relevant instance config changes
    Map<String, InstanceConfig> lastKnownInstanceConfigs = _lastKnownInstanceConfigsForTable.get(tableName);
    if (lastKnownInstanceConfigs == null || lastKnownInstanceConfigs.isEmpty()) {
      LOGGER.info("Routing table for table {} requires rebuild due to empty/null previous instance configs", tableName);
      return true;
    }

    // Gather relevant incoming instance configs
    Map<String, InstanceConfig> currentRelevantInstanceConfigs = new HashMap<>();
    for (InstanceConfig incomingInstanceConfig : instanceConfigs) {
      String instanceName = incomingInstanceConfig.getInstanceName();

      if (lastKnownInstanceConfigs.containsKey(instanceName)) {
        currentRelevantInstanceConfigs.put(instanceName, incomingInstanceConfig);
      }
    }

    // Did some instances lose their configuration?
    if (lastKnownInstanceConfigs.size() != currentRelevantInstanceConfigs.size()) {
      LOGGER.info(
          "Routing table for table {} requires rebuild due to having a different number of instance configs (known instance config count {}, current instance config count {})",
          tableName, lastKnownInstanceConfigs.size(), currentRelevantInstanceConfigs.size());
      return true;
    }

    // Did some instance change state?
    for (String instanceName : lastKnownInstanceConfigs.keySet()) {
      InstanceConfig previousInstanceConfig = lastKnownInstanceConfigs.get(instanceName);
      InstanceConfig currentInstanceConfig = currentRelevantInstanceConfigs.get(instanceName);

      // If it's the same znode, don't bother comparing the contents of the instance configs
      if (previousInstanceConfig.getRecord().getVersion() == currentInstanceConfig.getRecord().getVersion()) {
        continue;
      }

      // Check if the instance got enabled/disabled or started/stopped shutting down since the last update
      boolean wasEnabled = previousInstanceConfig.getInstanceEnabled();
      boolean isEnabled = currentInstanceConfig.getInstanceEnabled();

      String wasShuttingDown =
          previousInstanceConfig.getRecord().getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS);
      String isShuttingDown =
          currentInstanceConfig.getRecord().getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS);

      boolean instancesChanged =
          !EqualityUtils.isEqual(wasEnabled, isEnabled) || !EqualityUtils.isEqual(wasShuttingDown, isShuttingDown);

      if (instancesChanged) {
        LOGGER.info(
            "Routing table for table {} requires rebuild due to at least one instance changing state (instance {} enabled: {} -> {}; shutting down {} -> {})",
            tableName, instanceName, wasEnabled, isEnabled, wasShuttingDown, isShuttingDown);
        return true;
      } else {
        // Update the instance config in our last known instance config, since it hasn't changed
        _lastKnownInstanceConfigs.put(instanceName, currentInstanceConfig);
        for (String tableForInstance : _tablesForInstance.get(instanceName)) {
          _lastKnownInstanceConfigsForTable.get(tableForInstance).put(instanceName, currentInstanceConfig);
        }
      }
    }

    // No relevant changes, no need to update the routing table
    LOGGER.info("Routing table for table {} does not require a rebuild", tableName);
    return false;
  }

  private void buildRoutingTable(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigs) {
    // Save the current version number of the external view to avoid unnecessary routing table updates
    int externalViewRecordVersion = externalView.getRecord().getVersion();
    _lastKnownExternalViewVersionMap.put(tableName, externalViewRecordVersion);

    RoutingTableBuilder routingTableBuilder;
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    // Pick the appropriate routing table builder based on the table type
    if (CommonConstants.Helix.TableType.REALTIME.equals(tableType)) {
      routingTableBuilder = _realtimeHLCRoutingTableBuilder;
    } else {
      routingTableBuilder = _offlineRoutingTableBuilder;
    }

    LOGGER.info("Trying to compute routing table for table {} using {}", tableName, routingTableBuilder);
    long startTimeMillis = System.currentTimeMillis();

    try {
      Map<String, InstanceConfig> relevantInstanceConfigs = new HashMap<>();

      // Build a list of routing tables
      List<ServerToSegmentSetMap> serverToSegmentSetMap =
          routingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigs);

      // Keep track of the instance configs that are used in that routing table
      updateInstanceConfigsMapFromRoutingTables(relevantInstanceConfigs, instanceConfigs, serverToSegmentSetMap);

      _brokerRoutingTable.put(tableName, serverToSegmentSetMap);

      // If this is a realtime table, also build a LLC routing table
      if (CommonConstants.Helix.TableType.REALTIME.equals(tableType)) {
        _routingTableSelector.registerTable(tableName);

        try {
          // Build the routing table
          List<ServerToSegmentSetMap> llcserverToSegmentSetMap = _realtimeLLCRoutingTableBuilder
              .computeRoutingTableFromExternalView(tableName, externalView, instanceConfigs);

          // Keep track of the instance configs that are used in that routing table
          updateInstanceConfigsMapFromRoutingTables(relevantInstanceConfigs, instanceConfigs, llcserverToSegmentSetMap);

          _llcBrokerRoutingTable.put(tableName, llcserverToSegmentSetMap);
        } catch (Exception e) {
          LOGGER.error("Failed to compute LLC routing table for {}. Ignoring", tableName, e);
        }
      }

      // Save the instance configs used so that we can avoid unnecessary routing table updates later
      _lastKnownInstanceConfigsForTable.put(tableName, relevantInstanceConfigs);
      for (InstanceConfig instanceConfig : relevantInstanceConfigs.values()) {
        _lastKnownInstanceConfigs.put(instanceConfig.getInstanceName(), instanceConfig);
      }

      // Ensure this table is registered with all relevant instances
      for (String instanceName : relevantInstanceConfigs.keySet()) {
        Set<String> tablesForCurrentInstance = _tablesForInstance.get(instanceName);

        // Ensure there is a table set for this instance
        if (tablesForCurrentInstance == null) {
          synchronized (_tablesForInstance) {
            if (!_tablesForInstance.containsKey(instanceName)) {
              tablesForCurrentInstance = Sets.newConcurrentHashSet();
              _tablesForInstance.put(instanceName, tablesForCurrentInstance);
            } else {
              // Another thread has created a table set for this instance, use it
              tablesForCurrentInstance = _tablesForInstance.get(instanceName);
            }
          }
        }

        // Add the table to the set of tables for this instance
        tablesForCurrentInstance.add(tableName);
      }
    } catch (Exception e) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.ROUTING_TABLE_REBUILD_FAILURES, 1L);
      LOGGER.error("Failed to compute/update the routing table", e);

      // Mark the routing table as needing a rebuild
      _lastKnownExternalViewVersionMap.put(tableName, INVALID_EXTERNAL_VIEW_VERSION);
    }

    try {
      // We need to compute the time boundary only in two situations:
      // 1) We're adding/updating an offline table and there's a realtime table that we're serving
      // 2) We're adding a new realtime table and there's already an offline table, in which case we need to update the
      //    time boundary for the existing offline table
      String tableForTimeBoundaryUpdate = null;

      if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
        // Does a realtime table exist?
        String realtimeTableName =
            TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(TableNameBuilder.extractRawTableName(tableName));
        if (_brokerRoutingTable.containsKey(realtimeTableName)) {
          tableForTimeBoundaryUpdate = tableName;
        }
      }

      if (tableType == CommonConstants.Helix.TableType.REALTIME) {
        // Does an offline table exist?
        String offlineTableName =
            TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TableNameBuilder.extractRawTableName(tableName));
        if (_brokerRoutingTable.containsKey(offlineTableName)) {
          // Is there no time boundary?
          if (_timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName) == null) {
            tableForTimeBoundaryUpdate = tableName;
          }
        }
      }

      if (tableForTimeBoundaryUpdate != null) {
        LOGGER.info("Trying to compute time boundary service for table {}", tableForTimeBoundaryUpdate);
        long timeBoundaryUpdateStart = System.currentTimeMillis();
        _timeBoundaryService.updateTimeBoundaryService(externalView);
        long timeBoundaryUpdateEnd = System.currentTimeMillis();
        LOGGER.info("Computed the time boundary for table {} in {} ms", tableForTimeBoundaryUpdate,
            (timeBoundaryUpdateEnd - timeBoundaryUpdateStart));
      } else {
        LOGGER.info("No need to update time boundary for table {}", tableName);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to update the TimeBoundaryService", e);
    }

    long updateTime = System.currentTimeMillis() - startTimeMillis;

    if (_brokerMetrics != null) {
      _brokerMetrics.addTimedValue(BrokerTimer.ROUTING_TABLE_UPDATE_TIME, updateTime, TimeUnit.MILLISECONDS);
    }

    LOGGER.info("Routing table update for table {} completed in {} ms", tableName, updateTime);
  }

  private void updateInstanceConfigsMapFromRoutingTables(Map<String, InstanceConfig> relevantInstanceConfigs,
      List<InstanceConfig> instanceConfigs, List<ServerToSegmentSetMap> serverToSegmentSetMaps) {
    Set<String> relevantInstanceNames = new HashSet<>();

    // Gather all the instance names contained in the routing table
    for (ServerToSegmentSetMap serverToSegmentSetMap : serverToSegmentSetMaps) {
      relevantInstanceNames.addAll(serverToSegmentSetMap.getServerSet());
    }

    // Update the relevant instance config map with the instance configs given
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (relevantInstanceNames.contains(instanceConfig.getInstanceName())) {
        relevantInstanceConfigs.put(instanceConfig.getInstanceName(), instanceConfig);
      }
    }
  }

  public void markDataResourceOffline(String tableName) {
    LOGGER.info("Trying to remove data table from broker for {}", tableName);
    _brokerRoutingTable.remove(tableName);
    _lastKnownExternalViewVersionMap.remove(tableName);
    _lastKnownInstanceConfigsForTable.remove(tableName);
    _timeBoundaryService.remove(tableName);

    // Remove table from all instances
    synchronized (_tablesForInstance) {
      for (String instanceName : _brokerRoutingTable.keySet()) {
        Set<String> tablesForCurrentInstance = _tablesForInstance.get(instanceName);
        if (tablesForCurrentInstance.contains(tableName)) {
          tablesForCurrentInstance.remove(tableName);
        }

        if (tablesForCurrentInstance.isEmpty()) {
          _lastKnownInstanceConfigs.remove(instanceName);
        }
      }
    }
  }

  public void processExternalViewChange() {
    long startTime = System.currentTimeMillis();

    // Get list of tables that we're serving
    List<String> tablesServed = new ArrayList<>(_lastKnownExternalViewVersionMap.keySet());

    if (tablesServed.isEmpty()) {
      return;
    }

    // Build list of external views to fetch
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder propertyKeyBuilder = helixDataAccessor.keyBuilder();

    List<String> externalViewPaths = new ArrayList<>(tablesServed.size());
    for (String tableName : tablesServed) {
      PropertyKey propertyKey = propertyKeyBuilder.externalView(tableName);
      externalViewPaths.add(propertyKey.getPath());
    }

    // Get znode stats for all tables that we're serving
    long statStartTime = System.currentTimeMillis();
    Stat[] externalViewStats =
        helixDataAccessor.getBaseDataAccessor().getStats(externalViewPaths, AccessOption.PERSISTENT);
    long statEndTime = System.currentTimeMillis();

    // Make a list of external views that changed
    List<String> tablesThatChanged = new ArrayList<>();

    long evCheckStartTime = System.currentTimeMillis();
    for (int i = 0; i < externalViewStats.length; i++) {
      Stat externalViewStat = externalViewStats[i];
      if (externalViewStat != null) {
        String currentTableName = tablesServed.get(i);
        int currentExternalViewVersion = externalViewStat.getVersion();
        int lastKnownExternalViewVersion = _lastKnownExternalViewVersionMap.get(currentTableName);

        if (lastKnownExternalViewVersion != currentExternalViewVersion) {
          tablesThatChanged.add(currentTableName);
        }
      }
    }
    long evCheckEndTime = System.currentTimeMillis();

    // Fetch the instance configs and update the routing tables for the tables that changed
    long icFetchTime = 0;
    long rebuildStartTime = System.currentTimeMillis();
    if (!tablesThatChanged.isEmpty()) {
      // Fetch instance configs
      long icFetchStart = System.currentTimeMillis();
      List<InstanceConfig> instanceConfigs = helixDataAccessor.getChildValues(propertyKeyBuilder.instanceConfigs());
      long icFetchEnd = System.currentTimeMillis();
      icFetchTime = icFetchEnd - icFetchStart;

      for (String tableThatChanged : tablesThatChanged) {
        // We ignore the external views given by Helix on external view change and fetch the latest version as our
        // version of Helix (0.6.5) does not batch external view change messages.
        ExternalView externalView = helixDataAccessor.getProperty(propertyKeyBuilder.externalView(tableThatChanged));

        buildRoutingTable(tableThatChanged, externalView, instanceConfigs);
      }
    }
    long rebuildEndTime = System.currentTimeMillis();

    long endTime = System.currentTimeMillis();
    LOGGER.warn(
        "Processed external view change in {} ms (stat {} ms, EV check {} ms, IC fetch {} ms, rebuild {} ms), routing tables rebuilt for tables {}, {} / {} routing tables rebuilt",
        (endTime - startTime), (statEndTime - statStartTime), (evCheckEndTime - evCheckStartTime), icFetchTime,
        (rebuildEndTime - rebuildStartTime), tablesThatChanged, tablesThatChanged.size(), tablesServed.size());
  }

  public void processInstanceConfigChange() {
    long startTime = System.currentTimeMillis();

    // Get stats for all relevant instance configs
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder propertyKeyBuilder = helixDataAccessor.keyBuilder();
    List<String> instancesUsed = new ArrayList<>(_tablesForInstance.keySet());
    List<String> instancePaths = new ArrayList<>(instancesUsed.size());

    for (String instanceName : instancesUsed) {
      PropertyKey propertyKey = propertyKeyBuilder.instanceConfig(instanceName);
      instancePaths.add(propertyKey.getPath());
    }

    if (instancePaths.isEmpty()) {
      return;
    }

    long statFetchStart = System.currentTimeMillis();
    Stat[] instanceConfigStats =
        helixDataAccessor.getBaseDataAccessor().getStats(instancePaths, AccessOption.PERSISTENT);
    long statFetchEnd = System.currentTimeMillis();

    // Make a list of instance configs that changed
    long icConfigCheckStart = System.currentTimeMillis();
    List<String> instancesThatChanged = new ArrayList<>();

    for (int i = 0; i < instanceConfigStats.length; i++) {
      Stat instanceConfigStat = instanceConfigStats[i];
      if (instanceConfigStat != null) {
        String instanceName = instancesUsed.get(i);
        int currentInstanceConfigVersion = instanceConfigStat.getVersion();
        int lastKnownInstanceConfigVersion = _lastKnownInstanceConfigs.get(instanceName).getRecord().getVersion();

        if (currentInstanceConfigVersion != lastKnownInstanceConfigVersion) {
          instancesThatChanged.add(instanceName);
        }
      }
    }

    // Make a list of all tables affected by the instance config changes
    Set<String> affectedTables = new HashSet<>();
    for (String instanceName : instancesThatChanged) {
      affectedTables.addAll(_tablesForInstance.get(instanceName));
    }
    long icConfigCheckEnd = System.currentTimeMillis();

    // Update the routing tables
    long icFetchTime = 0;
    long evFetchTime = 0;
    long rebuildCheckTime = 0;
    long buildTime = 0;
    int routingTablesRebuiltCount = 0;
    if (!affectedTables.isEmpty()) {
      long icFetchStart = System.currentTimeMillis();
      List<InstanceConfig> instanceConfigs = helixDataAccessor.getChildValues(propertyKeyBuilder.instanceConfigs());
      long icFetchEnd = System.currentTimeMillis();
      icFetchTime = icFetchEnd - icFetchStart;

      for (String tableName : affectedTables) {
        long evFetchStart = System.currentTimeMillis();
        ExternalView externalView = helixDataAccessor.getProperty(propertyKeyBuilder.externalView(tableName));
        long evFetchEnd = System.currentTimeMillis();
        evFetchTime += evFetchEnd - evFetchStart;

        long rebuildCheckStart = System.currentTimeMillis();
        final boolean routingTableRebuildRequired =
            isRoutingTableRebuildRequired(tableName, externalView, instanceConfigs);
        long rebuildCheckEnd = System.currentTimeMillis();
        rebuildCheckTime += rebuildCheckEnd - rebuildCheckStart;

        if (routingTableRebuildRequired) {
          long rebuildStart = System.currentTimeMillis();
          buildRoutingTable(tableName, externalView, instanceConfigs);
          long rebuildEnd = System.currentTimeMillis();
          buildTime += rebuildEnd - rebuildStart;
          routingTablesRebuiltCount++;
        }
      }
    }
    long endTime = System.currentTimeMillis();

    LOGGER.warn(
        "Processed instance config change in {} ms (stat {} ms, IC check {} ms, IC fetch {} ms, EV fetch {} ms, rebuild check {} ms, rebuild {} ms), {} / {} routing tables rebuilt",
        (endTime - startTime), (statFetchEnd - statFetchStart), (icConfigCheckEnd - icConfigCheckStart), icFetchTime,
        evFetchTime, rebuildCheckTime, buildTime, routingTablesRebuiltCount, _lastKnownExternalViewVersionMap.size());
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

  @Override
  public String dumpSnapshot(String tableName)
      throws Exception {
    JSONObject ret = new JSONObject();
    JSONArray routingTableSnapshot = new JSONArray();

    for (String currentTable : _brokerRoutingTable.keySet()) {
      if (tableName == null || currentTable.startsWith(tableName)) {
        JSONObject tableEntry = new JSONObject();
        tableEntry.put("tableName", currentTable);

        JSONArray entries = new JSONArray();
        List<ServerToSegmentSetMap> routableTable = _brokerRoutingTable.get(currentTable);
        for (ServerToSegmentSetMap serverToInstaceMap : routableTable) {
          entries.put(new JSONObject(serverToInstaceMap.toString()));
        }
        tableEntry.put("routingTableEntries", entries);

        routingTableSnapshot.put(tableEntry);
      }
    }

    ret.put("routingTableSnapshot", routingTableSnapshot);

    routingTableSnapshot = new JSONArray();
    for (String currentTable : _llcBrokerRoutingTable.keySet()) {
      if (tableName == null || currentTable.startsWith(tableName)) {
        JSONObject tableEntry = new JSONObject();
        tableEntry.put("tableName", currentTable);

        JSONArray entries = new JSONArray();
        List<ServerToSegmentSetMap> routableTable = _llcBrokerRoutingTable.get(currentTable);
        for (ServerToSegmentSetMap serverToInstaceMap : routableTable) {
          entries.put(new JSONObject(serverToInstaceMap.toString()));
        }
        tableEntry.put("routingTableEntries", entries);

        routingTableSnapshot.put(tableEntry);
      }
    }
    ret.put("llcRoutingTableSnapshot", routingTableSnapshot);

    ret.put("host", NetUtil.getHostnameOrAddress());

    return ret.toString(2);
  }
}
