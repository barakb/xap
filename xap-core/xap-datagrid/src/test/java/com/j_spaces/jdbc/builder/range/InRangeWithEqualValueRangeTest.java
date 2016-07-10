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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by Tamir on 1/25/16.
 */
@com.gigaspaces.api.InternalApi
public class InRangeWithEqualValueRangeTest {

    private InRange absInRange;
    private InRange inRange;

    @Before
    public void setUp() throws Exception {
        absInRange = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
        inRange = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
    }

    @After
    public void tearDown() throws Exception {

    }

    // both ranges not have function
    // [value in (1, 2, 3, -1) & value=2]  ==> value=2
    @SuppressWarnings("Duplicates")
    @Test
    public void testIntersectionNoFunctionInRangeNoFunctionEqualValueRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 2);
        Range result = inRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(2));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges not have function
    // [value in (1, 2, 3, -1) & value=5]  ==> EMPTY_RANGE
    @Test
    public void testIntersectionNoFunctionInRangeNoFunctionEqualValueRangeNoMatch() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 5);
        Range result = inRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // this has function, but other range not
    // [ABS(value) in (1, 2, 3, -1) && value=2]  ==> value=2
    @SuppressWarnings("Duplicates")
    @Test
    public void testIntersectionWithEqualValueRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 2);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(2));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // this has function, but other range not
    // [ABS(value) in (1, 2, 3, -1) & value=5]  ==> value=EMPTY_RANGE
    @Test
    public void testIntersectionWithEqualValueRangeNoMatch() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", 5);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // this has function, but other range not
    // [ABS(value) in (1, 2, 3, -1) & value=-1]  ==> value=1
    @Test
    public void testIntersectionWithEqualValueRangeResultIsEqualValueRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", -1);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(1));
    }

    // this NOT have function, but other range has
    // [value in (1, 2, 3, -1) & ABS(value)=1]  ==> value in (1, -1)
    @Test
    public void testABSIntersectionWithEqualValueRangeResultIsInRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 1);
        Range result = inRange.intersection(evr);
        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 2);
        Assert.assertTrue(((InRange) result).getInValues().contains(1));
        Assert.assertTrue(((InRange) result).getInValues().contains(-1));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // this NOT have function, but other range has
    // [value in (1, 2, 3, -1) & ABS(value)=5]  ==> EMPTY_RANGE
    @Test
    public void testABSIntersectionWithEqualValueRangeResultIsEmptyRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 5);
        Range result = inRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // this NOT have function, but other range has
    // [value in (1, 2, 3, -1) & ABS(value)=2]  ==> value=2
    @Test
    public void testABSIntersectionWithEqualValueRangeResultIsEqualValueRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 2);
        Range result = inRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(2));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges has function - the same function
    // [ABS(value) in (1, 2, 3, -1) & ABS(value)=1]  ==> ABS(value)=1
    @Test
    public void testABSIntersectionWithEqualValueRangeSameFunctionsResultIsEqualValueRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 1);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(1));
    }

    // both ranges has function - the same function
    // [ABS(value) in (1, 2, 3, -1) & ABS(value)=5]  ==> EMPTY_RANGE
    @Test
    public void testABSIntersectionWithEqualValueRangeSameFunctionsResultIsEmptyRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.ABS, 5);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both ranges has function - the but different functions
    // [ABS(value) in (1, 2, 3, -1) & CEIL(value)=5]  ==> composite_range(this,range)
    @Test
    public void testABSIntersectionWithEqualValueRangeNOTSameFunctionsResultIsEmptyRange() throws Exception {
        EqualValueRange evr = new EqualValueRange("foo", FunctionUtils.CEIL, 5);
        Range result = absInRange.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}