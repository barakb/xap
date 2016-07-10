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
 * Created by Tamir on 2/3/16.
 */
@com.gigaspaces.api.InternalApi
public class SegmentWithSegamentTest {

    private SegmentRange segmentRangeTwoLimitsNoFunction; // 0 <= x <= 100
    private SegmentRange ABSsegmentRangeTwoLimitsNoFunction; // ABS 0 <= x <= 100

    @Before
    public void setUp() throws Exception {
        segmentRangeTwoLimitsNoFunction = new SegmentRange("foo", 0, true, 100, true);
        ABSsegmentRangeTwoLimitsNoFunction = new SegmentRange("foo", FunctionUtils.ABS, 0, true, 100, true);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value(0 <= x)==> value(0 <= x <= 100)
    @Test
    public void noFunction() throws Exception {
        SegmentRange sr = new SegmentRange("col", 0, true, null, false);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertTrue(result.isSegmentRange());
        Assert.assertTrue(((SegmentRange) result).getMax().equals(segmentRangeTwoLimitsNoFunction.getMax()));
        Assert.assertTrue(((SegmentRange) result).isIncludeMax() == (segmentRangeTwoLimitsNoFunction.isIncludeMax()));
        Assert.assertTrue(((SegmentRange) result).getMin().equals(segmentRangeTwoLimitsNoFunction.getMin()));
        Assert.assertTrue(((SegmentRange) result).isIncludeMin() == (segmentRangeTwoLimitsNoFunction.isIncludeMin()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // value(0 <= x <= 100) & value(200 <= x)==> empty range
    @Test
    public void noFunction_1() throws Exception {
        SegmentRange sr = new SegmentRange("col", 200, true, null, false);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // left side has function
    // ABS(value)(0 <= x <= 100) & value(100 <= x)==> empty range
    @Test
    public void leftSideWithFunction() throws Exception {
        SegmentRange sr = new SegmentRange("col", 100, true, null, false);
        Range result = ABSsegmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // right side has function - same as left side
    // value(0 <= x <= 100) & ABS(value)(100 <= x)==> empty range
    @Test
    public void rightSideWithFunction() throws Exception {
        SegmentRange sr = new SegmentRange("col", FunctionUtils.ABS, 100, true, null, false);
        Range result = segmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has same functions
    // ABS(value)(0 <= x <= 100) & ABS(value)(50 <= x) ==> ABS(value)(50 <= x <= 100)
    @Test
    public void bothSideHasSameFunctions() throws Exception {
        SegmentRange sr = new SegmentRange("col", FunctionUtils.ABS, 50, true, null, true);
        Range result = ABSsegmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertTrue(result.isSegmentRange());
        Assert.assertTrue(((SegmentRange) result).getMin().equals(50));
        Assert.assertTrue(((SegmentRange) result).isIncludeMin());
        Assert.assertTrue(((SegmentRange) result).getMax().equals(100));
        Assert.assertTrue(((SegmentRange) result).isIncludeMax());
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
    }

    // both sides has same functions
    // ABS(value)(0 <= x <= 100) & ABS(value)(0 < x < 5) ==> ABS(value)(0 < x < 5)
    @Test
    public void bothSideHasSameFunctions_1() throws Exception {
        SegmentRange sr = new SegmentRange("col", FunctionUtils.ABS, 0, true, 5, false);
        Range result = ABSsegmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertTrue(result.isSegmentRange());
        Assert.assertTrue(((SegmentRange) result).getMax().equals(sr.getMax()));
        Assert.assertTrue(((SegmentRange) result).isIncludeMax() == sr.isIncludeMax());
        Assert.assertTrue(((SegmentRange) result).getMin().equals(sr.getMin()));
        Assert.assertTrue(((SegmentRange) result).isIncludeMin() == sr.isIncludeMin());
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
    }

    // both sides has different functions
    // ABS(value)(0 <= x <= 100) & CEIL(value)(0 < x < 5) ==> composeite range
    @Test
    public void bothSideHasDifferentFunctions() throws Exception {
        SegmentRange sr = new SegmentRange("col", FunctionUtils.CEIL, 0, true, 5, false);
        Range result = ABSsegmentRangeTwoLimitsNoFunction.intersection(sr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}
