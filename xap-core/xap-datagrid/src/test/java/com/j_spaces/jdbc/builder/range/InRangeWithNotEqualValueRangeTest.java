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
public class InRangeWithNotEqualValueRangeTest {

    private InRange inRange;
    private InRange absInRange;
    private InRange doubleInRange;
    private InRange doubleRoundInRange;

    @Before
    public void setUp() throws Exception {
        inRange = new InRange("foo", new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
        absInRange = new InRange("foo", FunctionUtils.ABS, new HashSet<Integer>(Arrays.asList(-1, 2, 3, 1)));
        doubleInRange = new InRange("foo", new HashSet<Double>(Arrays.asList(-1.8, 2.2, 3.3, 1.1)));
        doubleRoundInRange = new InRange("foo", FunctionUtils.ROUND, new HashSet<Double>(Arrays.asList(-1.8, 2.2, 3.3, 1.1)));
    }

    // both ranges no have function
    // [value in (1, 2, 3, -1) & value!=2]  ==> value in (1, 3, -1)
    @Test
    public void testIntersectionNoFunctionOnBothSide() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 2);
        Range result = inRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(1));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(((InRange) result).getInValues().contains(-1));
        Assert.assertFalse(((InRange) result).getInValues().contains(nevr.getValue()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both ranges no have function
    // [value in (1, 2, 3, -1) & value!=5]  ==> value in (1, 2, 3, -1)
    @Test
    public void testIntersectionNoFunctionInRangeNoFunctionEqualValueRange() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 5);
        Range result = inRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 4);
        //noinspection unchecked
        Assert.assertTrue(((InRange) result).getInValues().containsAll(inRange.getInValues()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only inRange has function
    // [ABS(value) in (1, 2, 3, -1) & value!=1]  ==> composite_range(inRange,NotEqualValueRange)
    // e.g ABS(value!=1) = 1 or ABS(value!=2) ..
    @Test
    public void testIntersectionFunctionOnlyOnInRange_1() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", 1);
        Range result = absInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only NotEqualValueRange has function
    // [value in (1, 2, 3, -1) & ABS(value)!=1]  ==> value in (2, 3)
    @Test
    public void testABSIntersectionFunctionOnlyOnNotEqualValueRange() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 1);
        Range result = inRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 2);
        Assert.assertTrue(((InRange) result).getInValues().contains(2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only NotEqualValueRange has function
    // [value in (-1.8, 2.2 ,3.3, 1.1) && ROUND(value) != 1.0]  ==> value in (-1.8, 2.2 ,3.3)
    @Test
    public void testROUNDIntersectionFunctionOnlyOnNotEqualValueRange() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ROUND, 1.0);
        Range result = doubleInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(-1.8));
        Assert.assertTrue(((InRange) result).getInValues().contains(2.2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3.3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // only NotEqualValueRange has function
    // [value in (-1.8, 1.1, 2.2, 3.3) & ROUND(value)!=5]  ==> value in (-1.8, 1.1, 2.2, 3.3)
    @Test
    public void testROUNDIntersectionFunctionOnlyOnNotEqualValueRange_1() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ROUND, 5L);
        Range result = doubleInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 4);
        //noinspection unchecked
        Assert.assertTrue(((InRange) result).getInValues().containsAll(doubleInRange.getInValues()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has same function
    // [ABS(value) in (-1, 1, 2, 3) & ABS(value)!=1]  ==> value in (-1, 2, 3)
    @Test
    public void testABSIntersectionBothSideHasSameFunction() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 1);
        Range result = absInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(-1));
        Assert.assertTrue(((InRange) result).getInValues().contains(2));
        Assert.assertTrue(((InRange) result).getInValues().contains(3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both sides has same function
    // [ABS(value) in (-1, 1, 2, 3) & ABS(value)!=5]  ==> value in (-1, 1, 2, 3)
    @Test
    public void testABSIntersectionBothSideHasSameFunction_1() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ABS, 5);
        Range result = absInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        //noinspection unchecked
        Assert.assertTrue(((InRange) result).getInValues().containsAll(absInRange.getInValues()));
        Assert.assertTrue(result.getFunctionCallDescription() == null);

    }

    // both sides has same function
    // [ROUND(value) in (-1.8, 1.1, 2.2, 3.3) & ROUND(value)!=2.2]  ==> value in (-1.8, 1.1, 3.3)
    @Test
    public void testABSIntersectionBothSideHasSameFunction_2() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ROUND, 2.2);
        Range result = doubleRoundInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), InRange.class);
        Assert.assertTrue(((InRange) result).getInValues().size() == 3);
        Assert.assertTrue(((InRange) result).getInValues().contains(-1.8));
        Assert.assertTrue(((InRange) result).getInValues().contains(1.1));
        Assert.assertTrue(((InRange) result).getInValues().contains(3.3));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has function, but different function
    // [ABS(value) in (-1.8, 1.1, 2.2, 3.3) & ROUND(value)!=2.2]  ==> composite_range()
    @Test
    public void testABSIntersectionBothSideHasDifferentFunction() throws Exception {
        //noinspection SpellCheckingInspection
        NotEqualValueRange nevr = new NotEqualValueRange("foo", FunctionUtils.ROUND, 2.2);
        Range result = absInRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}