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
 * Created by Tamir on 1/27/16.
 */
@com.gigaspaces.api.InternalApi
public class SegmentRangeWithEqaulValueRangeTest {

    private SegmentRange segmentRangeTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange segmentRangeTwoLimitsNoFunction_2; // -50 <= x <= 50
    private SegmentRange segmentRangeOnlyLowerLimitNoFunction; // 0 <= x
    private SegmentRange segmentRangeOnlyLowerLimitNoFunction_2; // 0 <= x
    private SegmentRange segmentRangeOnlyLowerLimitABS; // ABS 0 <= x
    private SegmentRange segmentRangeOnlyLowerLimitFoor; // FLOOR 0 <= x
    private SegmentRange segmentRangeCeilTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange segmentRangeAbsTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange segmentRangeAbsTwoLimitsNoFunction_2; // 0 <= x <= 100

    @Before
    public void setUp() throws Exception {
        segmentRangeTwoLimitsNoFunction = new SegmentRange("foo", 0, true, 100, true);
        segmentRangeTwoLimitsNoFunction_2 = new SegmentRange("foo", -50, true, 50, true);
        segmentRangeOnlyLowerLimitNoFunction = new SegmentRange("foo", 0, true, null, false);
        segmentRangeOnlyLowerLimitNoFunction_2 = new SegmentRange("foo", 10, true, null, false);
        segmentRangeOnlyLowerLimitABS = new SegmentRange("foo", FunctionUtils.ABS, 0, true, null, false);
        segmentRangeOnlyLowerLimitFoor = new SegmentRange("foo", FunctionUtils.FLOOR, 0.0, true, null, false);
        segmentRangeCeilTwoLimitsNoFunction = new SegmentRange("foo", FunctionUtils.CEIL, 0.0, true, 100.0, true);
        segmentRangeAbsTwoLimitsNoFunction = new SegmentRange("foo", FunctionUtils.ABS, 0, true, 100, true);
        segmentRangeAbsTwoLimitsNoFunction_2 = new SegmentRange("foo", FunctionUtils.ABS, 10, true, 100, true);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value=2 ==> value=2
    @SuppressWarnings("Duplicates")
    @Test
    public void testIntersectionNoFunctions() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 2);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(2));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value=200 ==> EMPTY_RANGE
    @Test
    public void testIntersectionNoFunctions_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 200);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // not include upper limit
    // value(0 <= x < 100) & value=100 ==> EMPTY_RANGE
    @Test
    public void testIntersectionNoFunctions_2() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 100);
        segmentRangeTwoLimitsNoFunction.setIncludeMax(false);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // no upper limit
    // value(0 <= x) & value=-1 ==> EMPTY_RANGE
    @Test
    public void testIntersectionNoFunctions_3() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", -1);
        Range result = segmentRangeOnlyLowerLimitNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // no upper limit
    // value(0 <= x) & value=1000 ==> value=1000
    @Test
    public void testIntersectionNoFunctions_4() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 1000);
        Range result = segmentRangeOnlyLowerLimitNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(1000));
    }

    // only SegmentRange has function
    // ABA(value) (0 <= x <= 100) & value=-50 ==> value=-50
    @Test
    public void testIntersectionSegmentRangeHasFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", -50);
        Range result = segmentRangeAbsTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(-50));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }


    // only SegmentRange has function
    // CEIL(value) (0 <= x <= 100) & value=99.9 ==> value=99.9
    @Test
    public void testIntersectionSegmentRangeHasFunction_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 99.9);
        Range result = segmentRangeCeilTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(99.9));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // only SegmentRange has function
    // CEIL(value) (0 <= x <= 100) & value=99.9 ==> value=99.9
    @Test
    public void testIntersectionSegmentRangeHasFunction_2() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 100.1);
        Range result = segmentRangeCeilTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // only SegmentRange has function
    // FLOOR(value) (0 <= x) & value=0.1 ==> value=0.1
    @Test
    public void testIntersectionSegmentRangeHasFunction_3() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 0.1);
        Range result = segmentRangeOnlyLowerLimitFoor.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(0.1));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // only SegmentRange has function
    // FLOOR(value) (0 <= x) & value=-0.1 ==> value=0.1
    @Test
    public void testIntersectionSegmentRangeHasFunction_4() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", -0.1);
        Range result = segmentRangeOnlyLowerLimitFoor.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // only EqualValueRange has function
    // value (-50 <= x <= 50) & ABS(value)=10 ==> composite
    @Test
    public void testIntersectionEqualValuetRangeHasFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 10);
        Range result = segmentRangeTwoLimitsNoFunction_2.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // only EqualValueRange has function
    // value (10 <= x) & ABS(value)=-11 ==> composite
    @Test
    public void testIntersectionEqualValuetRangeHasFunction_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, -11);
        Range result = segmentRangeOnlyLowerLimitNoFunction_2.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges have same function
    // ABS(value) (10 <= x <= 100) & ABS(value)=50 ==> ABS(value)=50
    @Test
    public void testIntersectionBothHasSameFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 8);
        Range result = segmentRangeAbsTwoLimitsNoFunction_2.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
    }

    // both ranges have same function
    // ABS(value) (0 <= x) & ABS(value)=50 ==> ABS(value)=50
    @Test
    public void testIntersectionBothHasSameFunction_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 50);
        Range result = segmentRangeAbsTwoLimitsNoFunction.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(50));
        Assert.assertTrue(result.getFunctionCallDescription().equals(evr.getFunctionCallDescription()));
    }

    // both ranges have function, but different
    // ABS(value) (0 <= x <= 100) & CEIL(value)=50 ==> ABS(value)=50
    @Test
    public void testIntersectionBothHasDifferentFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.CEIL, 50);
        Range result = segmentRangeOnlyLowerLimitABS.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}