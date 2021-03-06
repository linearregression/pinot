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
package com.linkedin.pinot.operator.aggregation;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.aggregation.groupby.DefaultGroupKeyGenerator;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.util.TestDataRecordReader;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DefaultGroupKeyGeneratorTest {
  private static final String SEGMENT_NAME = "DefaultGroupKeyGeneratorTestSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final int NUM_ROWS = 1000;
  private static final int UNIQUE_ROWS = 100;
  private static final int MAX_STEP_LENGTH = 1000;
  private static final int MAX_NUM_MULTI_VALUES = 10;
  private static final String[] SINGLE_VALUE_COLUMNS = {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
  private static final String[] MULTI_VALUE_COLUMNS = {"m1", "m2"};

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);
  private final String _errorMessage = "Random seed is: " + _randomSeed;
  private DataFetcher _dataFetcher;

  private static final int TEST_LENGTH = 20;
  private final int[] _testDocIdSet = new int[TEST_LENGTH];
  private final int[] _singleValueGroupKeyBuffer = new int[TEST_LENGTH];
  private final int[][] _multiValueGroupKeyBuffer = new int[TEST_LENGTH][];

  @BeforeClass
  private void setup() throws Exception {
    GenericRow[] segmentData = new GenericRow[NUM_ROWS];
    int value = _random.nextInt(MAX_STEP_LENGTH);

    // Generate random values for the segment.
    for (int i = 0; i < UNIQUE_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      for (String singleValueColumn : SINGLE_VALUE_COLUMNS) {
        map.put(singleValueColumn, value);
        value += 1 + _random.nextInt(MAX_STEP_LENGTH);
      }
      for (String multiValueColumn : MULTI_VALUE_COLUMNS) {
        int numMultiValues = 1 + _random.nextInt(MAX_NUM_MULTI_VALUES);
        Integer[] values = new Integer[numMultiValues];
        for (int k = 0; k < numMultiValues; k++) {
          values[k] = value;
          value += 1 + _random.nextInt(MAX_STEP_LENGTH);
        }
        map.put(multiValueColumn, values);
      }
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      segmentData[i] = genericRow;
    }
    for (int i = UNIQUE_ROWS; i < NUM_ROWS; i += UNIQUE_ROWS) {
      System.arraycopy(segmentData, 0, segmentData, i, UNIQUE_ROWS);
    }

    // Create an index segment with the random values.
    Schema schema = new Schema();
    for (String singleValueColumn : SINGLE_VALUE_COLUMNS) {
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(singleValueColumn, FieldSpec.DataType.INT, true);
      schema.addField(dimensionFieldSpec);
    }
    for (String multiValueColumn : MULTI_VALUE_COLUMNS) {
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(multiValueColumn, FieldSpec.DataType.INT, false);
      schema.addField(dimensionFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new TestDataRecordReader(schema, segmentData));
    driver.build();

    IndexSegment indexSegment = Loaders.IndexSegment.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);

    // Get a data fetcher for the index segment.
    Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String column : indexSegment.getColumnNames()) {
      dataSourceMap.put(column, indexSegment.getDataSource(column));
    }
    _dataFetcher = new DataFetcher(dataSourceMap);

    // Generate a random test doc id set.
    int num1 = _random.nextInt(50);
    int num2 = num1 + 1 + _random.nextInt(50);
    for (int i = 0; i < 20; i += 2) {
      _testDocIdSet[i] = num1 + 50 * i;
      _testDocIdSet[i + 1] = num2 + 50 * i;
    }
  }

  @Test
  public void testArrayBasedSingleValue() {
    // Cardinality product < threshold.
    String[] groupByColumns = {"s1"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    Assert.assertEquals(defaultGroupKeyGenerator.getGlobalGroupKeyUpperBound(), 100, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 100, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), false, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _singleValueGroupKeyBuffer);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 100, _errorMessage);
    compareSingleValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), 2);
  }

  @Test
  public void testLongMapBasedSingleValue() {
    // Cardinality product > threshold.
    String[] groupByColumns = {"s1", "s2", "s3", "s4"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    Assert.assertEquals(defaultGroupKeyGenerator.getGlobalGroupKeyUpperBound(), 100000000, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), false, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _singleValueGroupKeyBuffer);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 2, _errorMessage);
    compareSingleValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), 2);
  }

  @Test
  public void testArrayMapBasedSingleValue() {
    // Cardinality product > Long.MAX_VALUE.
    String[] groupByColumns = {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    Assert.assertEquals(defaultGroupKeyGenerator.getGlobalGroupKeyUpperBound(), Integer.MAX_VALUE, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), false, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _singleValueGroupKeyBuffer);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 2, _errorMessage);
    compareSingleValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), 2);
  }

  /**
   * Helper method to compare the values inside the single value group key buffer.
   *
   * All odd number index values should be the same, all even number index values should be the same.
   * Odd number index values should be different from even number index values.
   */
  private void compareSingleValueBuffer() {
    Assert.assertTrue(_singleValueGroupKeyBuffer[0] != _singleValueGroupKeyBuffer[1], _errorMessage);
    for (int i = 0; i < 20; i += 2) {
      Assert.assertEquals(_singleValueGroupKeyBuffer[i], _singleValueGroupKeyBuffer[0], _errorMessage);
      Assert.assertEquals(_singleValueGroupKeyBuffer[i + 1], _singleValueGroupKeyBuffer[1], _errorMessage);
    }
  }

  @Test
  public void testArrayBasedMultiValue() {
    // Cardinality product < threshold.
    String[] groupByColumns = {"m1"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    int groupKeyUpperBound = defaultGroupKeyGenerator.getGlobalGroupKeyUpperBound();
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), groupKeyUpperBound, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), true, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _multiValueGroupKeyBuffer);
    int numUniqueKeys = _multiValueGroupKeyBuffer[0].length + _multiValueGroupKeyBuffer[1].length;
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), groupKeyUpperBound, _errorMessage);
    compareMultiValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), numUniqueKeys);
  }

  @Test
  public void testLongMapBasedMultiValue() {
    // Cardinality product > threshold.
    String[] groupByColumns = {"m1", "m2", "s1", "s2"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), true, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _multiValueGroupKeyBuffer);
    int numUniqueKeys = _multiValueGroupKeyBuffer[0].length + _multiValueGroupKeyBuffer[1].length;
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numUniqueKeys, _errorMessage);
    compareMultiValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), numUniqueKeys);
  }

  @Test
  public void testArrayMapBasedMultiValue() {
    // Cardinality product > Long.MAX_VALUE.
    String[] groupByColumns = {"m1", "m2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};

    // Test initial status.
    DefaultGroupKeyGenerator defaultGroupKeyGenerator = new DefaultGroupKeyGenerator(_dataFetcher, groupByColumns);
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);
    Assert.assertEquals(defaultGroupKeyGenerator.hasMultiValueGroupByColumn(), true, _errorMessage);

    // Test group key generation.
    defaultGroupKeyGenerator.generateKeysForDocIdSet(_testDocIdSet, 0, TEST_LENGTH, _multiValueGroupKeyBuffer);
    int numUniqueKeys = _multiValueGroupKeyBuffer[0].length + _multiValueGroupKeyBuffer[1].length;
    Assert.assertEquals(defaultGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numUniqueKeys, _errorMessage);
    compareMultiValueBuffer();
    testGetUniqueGroupKeys(defaultGroupKeyGenerator.getUniqueGroupKeys(), numUniqueKeys);
  }

  /**
   * Helper method to compare the values inside the multi value group key buffer.
   *
   * All odd number index values should be the same, all even number index values should be the same.
   * Odd number index values should be different from even number index values.
   */
  private void compareMultiValueBuffer() {
    int length0 = _multiValueGroupKeyBuffer[0].length;
    int length1 = _multiValueGroupKeyBuffer[1].length;
    int compareLength = Math.min(length0, length1);
    for (int i = 0; i < compareLength; i++) {
      Assert.assertTrue(_multiValueGroupKeyBuffer[0][i] != _multiValueGroupKeyBuffer[1][i], _errorMessage);
    }
    for (int i = 0; i < 20; i += 2) {
      for (int j = 0; j < length0; j++) {
        Assert.assertEquals(_multiValueGroupKeyBuffer[i][j], _multiValueGroupKeyBuffer[0][j], _errorMessage);
      }
      for (int j = 0; j < length1; j++) {
        Assert.assertEquals(_multiValueGroupKeyBuffer[i + 1][j], _multiValueGroupKeyBuffer[1][j], _errorMessage);
      }
    }
  }

  /**
   * Helper method to test the group key iterator returned by getUniqueGroupKeys().
   *
   * @param groupKeyIterator group key iterator.
   * @param numUniqueKeys number of unique keys.
   */
  private void testGetUniqueGroupKeys(Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator, int numUniqueKeys) {
    int count = 0;
    Set<Integer> idSet = new HashSet<>();
    Set<String> groupKeySet = new HashSet<>();

    while (groupKeyIterator.hasNext()) {
      count++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      idSet.add(groupKey.getFirst());
      groupKeySet.add(groupKey.getStringKey());
    }

    Assert.assertEquals(count, numUniqueKeys, _errorMessage);
    Assert.assertEquals(idSet.size(), numUniqueKeys, _errorMessage);
    Assert.assertEquals(groupKeySet.size(), numUniqueKeys, _errorMessage);
  }

  @AfterClass
  public void cleanUp() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
