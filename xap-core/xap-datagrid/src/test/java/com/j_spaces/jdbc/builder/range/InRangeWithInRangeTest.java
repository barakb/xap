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
 * Created by Tamir on 1/26/16.
 */
@com.gigaspaces.api.InternalApi
public class InRangeWithInRangeTest {

    private InRange inRange;
    private InRange absInRange;
    private InRange doubleInRange;
    private InRange floorInRange;
    private InRange ceilInRange;

    @Before
    public void setUp() throws Exception {
        inRange = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
        absInRange = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
        doubleInRange = new InRange("foo", new HashSet<Double>(Arrays.asList(-1.8, 2.0, 3.0, 1.1)));
        floorInRange = new InRange("foo", FunctionUtils.FLOOR, new HashSet<Double>(Arrays.asList(-1.0, 1.0, 2.0, 3.0)));
        ceilInRange = new InRange("foo", FunctionUtils.CEIL, new HashSet<Double>(Arrays.asList(-1.0, 1.0, 2.0, 3.0)));
    }

    // both ranges no have function
    // [value in (1, 2, 3, -1) & value in (1, 2, 3, -1)]  ==> value in (1, 2, 3, -1)
    @Test
    public void testIntersectionBothSidesNotHaveFunction() throws Exception {
        HashSet<Integer> integers = new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1));
        InRange ir = new InRange("foo", integers);
        Range result = inRange.intersection(ir);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 4);
        //noinspection unchecked
        Assert.assertTrue(((InRange) result).getInValues().containsAll(inRange.getInValues()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both ranges no have function
    // [value in (1, 2, 3, -1) & value in (1, 2, 3, -1)]  ==> value in (1, 2, 3, -1)
    @Test
    public void testIntersectionBothSidesNotHaveFunction_1() throws Exception {
        HashSet<Integer> integers = new HashSet<Integer>(Arrays.asList(-1, 2, 3));
        InRange ir = new InRange("foo", integers);
        Range result = inRange.intersection(ir);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(-1));
        Assert.assertTrue(((InRange) result).getInValues().contains(2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both ranges no have function
    // [value in (1, 2, 3, -1) & value in (4, 5)]  ==> EMPTY_RANGE
    @Test
    public void testIntersectionBothSidesNotHaveFunction_2() throws Exception {
        HashSet<Integer> integers = new HashSet<Integer>(Arrays.asList(4, 5));
        InRange ir = new InRange("foo", integers);
        Range result = inRange.intersection(ir);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only left or only right has function are equivalent
    // [CEIL(value) in (-1, 1, 2, 3) & value in (-2.3, 3.5, 2.7, 5.1)]  ==> value in (-1, 1, 2, 3,)
    @Test
    public void testIntersectionLeftInRangeHasFunction() throws Exception {
        HashSet<Double> doubles = new HashSet<Double>(Arrays.asList(-2.3, 3.5, 2.7, 5.1));
        InRange ir = new InRange("foo", doubles);
        Range result = ceilInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(2.7));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only left or only right has function are equivalent
    // [FLOOR(value) in (-1.0, 1.0, 2.0, 3.0) & value in (-1.3, 3.5, 2.7, 5.1)]  ==> value in (3.5, 2.7)
    @Test
    public void testIntersectionLeftInRangeHasFunction_1() throws Exception {
        HashSet<Double> doubles = new HashSet<Double>(Arrays.asList(-1.3, 3.5, 2.7, 5.1));
        InRange ir = new InRange("foo", doubles);
        Range result = floorInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 2);
        Assert.assertTrue(((InRange) result).getInValues().contains(3.5));
        Assert.assertTrue(((InRange) result).getInValues().contains(2.7));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only left or only right has function are equivalent
    // [value in (-1.0, -2.0, -3.0, -4.0) & FLOOR(value) in (-1.3, 3.5, 2.7, 5.1)]  ==> value in (3.5, 2.7)
    @Test
    public void testIntersectionRightInRangeHasFunction() throws Exception {
        HashSet<Double> doubles = new HashSet<Double>(Arrays.asList(-1.3, 3.5, 2.7, 5.1));
        InRange ir = new InRange("foo", FunctionUtils.FLOOR, doubles);
        Range result = doubleInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), EmptyRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only left or only right has function are equivalent
    // [value in (-1.8, 2.0 ,3.0, 1.1) && FLOOR(value) in (-2.0, 3.5, 2.7, 5.1)]  ==> value in (3.5, 2.7)
    @Test
    public void testIntersectionRightInRangeHasFunction_1() throws Exception {
        HashSet<Double> doubles = new HashSet<Double>(Arrays.asList(-2.0, 3.5, 2.7, 5.1));
        InRange ir = new InRange("foo", FunctionUtils.FLOOR, doubles);
        Range result = doubleInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(-1.8));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both side has the same function
    // [ABS(value) in (-1, 2, 3, 1) & ABS(value) in (2, 3, 5)]  ==> composite_range(this,range)
    @Test
    public void testIntersectionBothSidesHasSameFunction() throws Exception {
        HashSet<Integer> integers = new HashSet<Integer>(Arrays.asList(2, 3, 5));
        InRange ir = new InRange("foo", FunctionUtils.ABS, integers);
        Range result = absInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both side has different functions
    // [ABS(value) in (-1, 2, 3, 1) & FLOOR(value) in (2, 3, 5)]  ==> composite_range(this,range)
    @Test
    public void testIntersectionBothSidesHasDifferentFunction() throws Exception {
        HashSet<Integer> integers = new HashSet<Integer>(Arrays.asList(2, 3, 5));
        InRange ir = new InRange("foo", FunctionUtils.FLOOR, integers);
        Range result = absInRange.intersection(ir);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}