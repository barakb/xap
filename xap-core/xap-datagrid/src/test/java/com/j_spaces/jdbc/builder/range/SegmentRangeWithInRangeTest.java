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

import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by Tamir on 1/27/16.
 */
@com.gigaspaces.api.InternalApi
public class SegmentRangeWithInRangeTest {

    private SegmentRange segmentRangeTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange segmentRangeTwoLimitsNoFunction_1; //  x <= 0
    private SegmentRange segmentRangeOnlyLowerLimitABS; // ABS 0 <= x
    private SegmentRange segmentRangeOnlyLowerNotIncludeMinLimitFloor; // FLOOR 0 < x

    @Before
    public void setUp() throws Exception {
        segmentRangeTwoLimitsNoFunction = new SegmentRange("foo", 0, true, 100, true);
        segmentRangeTwoLimitsNoFunction_1 = new SegmentRange("foo", null, false, 0, true);
        segmentRangeOnlyLowerLimitABS = new SegmentRange("foo", FunctionUtils.ABS, 0, true, null, false);
        segmentRangeOnlyLowerNotIncludeMinLimitFloor = new SegmentRange("foo", FunctionUtils.FLOOR, 0.0, false, null, false);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value in (-1, 1, 2, 3) ==> value in (1, 2, 3)
    @Test
    public void testIntersectionNoFunctions() throws Exception {
        InRange ir = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeTwoLimitsNoFunction.intersection(ir);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(1));
        Assert.assertTrue(((InRange) result).getInValues().contains(2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // value(x <= 0) & value in (-1, 1, 2, 3) ==> value=-1
    @Test
    public void testIntersectionNoFunctions_1() throws Exception {
        InRange ir = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeTwoLimitsNoFunction_1.intersection(ir);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(-1));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // right side has function
    // value in (-1, 1, 2, 3) && ABS(value) => 0  ==> value in (-1, 1, 2, 3)
    @Test
    public void testIntersectionSegmentRangeHasFunction() throws Exception {
        InRange ir = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeOnlyLowerLimitABS.intersection(ir);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 4);
        Assert.assertTrue(((InRange) result).getInValues().contains(1));
        Assert.assertTrue(((InRange) result).getInValues().contains(-1));
        Assert.assertTrue(((InRange) result).getInValues().contains(2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // SegmentRange has function
    // FLOOR(value)(0 < x) & value in (-1.1, -5.0, -0.1, 0.1) ==> value=-1
    @Test
    public void testIntersectionSegmentRangeHasFunction_1() throws Exception {
        InRange ir = new InRange("foo", new HashSet<Double>(Arrays.asList(-1.1, -5.0, 0.1)));
        Range result = segmentRangeOnlyLowerNotIncludeMinLimitFloor.intersection(ir);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // InRange has function
    // value(0 <= x <= 100) & ABS(value) in (-1, 1, 2, 3) ==> composite_range
    @Test
    public void testIntersectionInRangeHasFunction() throws Exception {
        InRange ir = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeTwoLimitsNoFunction.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // InRange has function
    // value(0 <= x) & ABS(value) in (-1, 1, 2, 3) ==> composite_range
    @Test
    public void testIntersectionInRangeHasFunction_1() throws Exception {
        InRange ir = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeTwoLimitsNoFunction_1.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides with same function
    // ABS(value)(0 <= x) & ABS(value) in (-1, 1, 2, 3) ==> composite_range
    @Test
    public void testIntersectionBothSidesWithSameFunction() throws Exception {
        InRange ir = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeOnlyLowerLimitABS.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides with same function
    // ABS(value)(0 <= x) & CEIL(value) in (-1, 1, 2, 3) ==> composite_range
    @Test
    public void testIntersectionBothSidesWithDifferentFunction() throws Exception {
        InRange ir = new InRange("foo", FunctionUtils.CEIL, new HashSet<Integer>(Arrays.asList(-1, 1, 2, 3)));
        Range result = segmentRangeOnlyLowerLimitABS.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}