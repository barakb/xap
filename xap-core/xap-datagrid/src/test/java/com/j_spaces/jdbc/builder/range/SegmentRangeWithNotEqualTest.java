/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.jdbc.builder.range;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Tamir on 1/28/16.
 */
@com.gigaspaces.api.InternalApi
public class SegmentRangeWithNotEqualTest {

    private SegmentRange segmentRangeTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange segmentRangeTwoLimitsNoFunction_2; // -10 <= x <= 10
    private SegmentRange segmentRangeTwoLimitsABS; // -10 <= x <= 10
    private SegmentRange segmentRangeOnlyLowerLimitABS; // ABS 0 <= x

    @Before
    public void setUp() throws Exception {
        segmentRangeTwoLimitsNoFunction = new SegmentRange("foo", 0, true, 100, true);
        segmentRangeTwoLimitsNoFunction_2 = new SegmentRange("foo", -10, true, 10, true);
        segmentRangeTwoLimitsABS = new SegmentRange("foo", FunctionUtils.ABS, -10, true, 10, true);
        segmentRangeOnlyLowerLimitABS = new SegmentRange("foo", FunctionUtils.ABS, 0, true, null, false);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value!=2 ==> composite_range
    @Test
    public void testIntersectionNoFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 2);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value!=0 ==> value(0 < x <= 100)
    @Test
    public void testIntersectionNoFunctions_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 0);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(nevr);

        Assert.assertEquals(result.getClass(), SegmentRange.class);
        Assert.assertFalse(((SegmentRange) result).isIncludeMin());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // SegmentRange has function
    // ABS(value)(0 <= x) & value!=0 ==> CompositeRange
    @Test
    public void testIntersectionSegmentRangeHasfunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 0);
        Range result = segmentRangeOnlyLowerLimitABS.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // SegmentRange has function
    // ABS(value)(0 <= x) & value!=5 ==> CompositeRange
    @Test
    public void testIntersectionSegmentRangeHasfunction_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 5);
        Range result = segmentRangeOnlyLowerLimitABS.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // SegmentRange has function
    // value(0 <= x <= 100) & ABS(value)!=5 ==> composite_range
    @Test
    public void testIntersectionNotEqualRangeHasFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 5);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // SegmentRange has function
    // check only min/max values
    // (-10 <= x <= 10) && ABS(value)!=10 ==> composite_range
    // TODO check if not composite is ok
    @Test
    public void testIntersectionNotEqualRangeHasFunction_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 10);
        Range result = segmentRangeTwoLimitsNoFunction_2.intersection(nevr);

        Assert.assertEquals(result.getClass(), SegmentRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
        Assert.assertTrue(!((SegmentRange) result).isIncludeMax());
        Assert.assertTrue(!((SegmentRange) result).isIncludeMin());

    }

    // both sides has the same function
    // if [ABS(0) == min] or [ABS(0) == max] uninclude min/max
    // check only min/max values
    // ABS(value)(-10 <= x <= 10) & ABS(value)!=10 ==> ABS(value)(-10 <= x < 10)
    @Test
    public void testIntersectionBothSidesHasFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 10);
        Range result = segmentRangeTwoLimitsABS.intersection(nevr);

        Assert.assertEquals(result.getClass(), SegmentRange.class);
        Assert.assertFalse(((SegmentRange) result).isIncludeMax());
        Assert.assertTrue(result.getFunctionCallDescription().equals(segmentRangeTwoLimitsABS.getFunctionCallDescription()));

    }

    // both sides has the same function
    // if [ABS(0) == min] or [ABS(0) == max] uninclude min/max
    // check only min/max values
    // ABS(value)(-10 <= x <= 10) & ABS(value)!=5 ==> composite range
    @Test
    public void testIntersectionBothSidesHasFunction_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 5);
        Range result = segmentRangeTwoLimitsABS.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has different functions
    // if [ABS(0) == min] or [ABS(0) == max] uninclude min/max
    // check only min/max values
    // ABS(value)(-10 <= x <= 10) & ABS(value)!=5 ==> composite range
    @Test
    public void testIntersectionBothSidesHasDifferentFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.CEIL, 5);
        Range result = segmentRangeTwoLimitsABS.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}