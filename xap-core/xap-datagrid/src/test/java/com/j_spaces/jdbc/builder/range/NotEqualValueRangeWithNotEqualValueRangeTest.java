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
 * Created by Tamir on 2/4/16.
 */
@com.gigaspaces.api.InternalApi
public class NotEqualValueRangeWithNotEqualValueRangeTest {

    private NotEqualValueRange notEqualValueRange; // value != 5
    private NotEqualValueRange ABSnotEqualValueRange; // ABS(value) != 5

    @Before
    public void setUp() throws Exception {
        notEqualValueRange = new NotEqualValueRange("col", 5);
        ABSnotEqualValueRange = new NotEqualValueRange("col", FunctionUtils.ABS, 5);
    }

    // both ranges no have function
    // [value != 5 & value != 3 ]  ==> empty_range
    @Test
    public void noFunctions() throws Exception {
        NotEqualValueRange evr = new NotEqualValueRange("col", 3);
        Range result = notEqualValueRange.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges no have function
    // [value != 5 & value != 5 ]  ==> value != 5
    @Test
    public void noFunctions_1() throws Exception {
        NotEqualValueRange evr = new NotEqualValueRange("col", 5);
        Range result = notEqualValueRange.intersection(evr);

        Assert.assertEquals(result.getClass(), NotEqualValueRange.class);
        Assert.assertTrue(((NotEqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // left side has function
    // [ABS(value) != 5 & value != -3 ]  ==> composite range
    @Test
    public void leftSideHasFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", -3);
        Range result = ABSnotEqualValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // right side has function - equal to left side
    // [value != 5 & ABS(value) != -3 ]  ==> composite range
    @Test
    public void rightSideHasFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, -3);
        Range result = notEqualValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has functions - same function
    // [ABS(value)!=5 & ABS(value)!=5]  ==> ABS(value)!=5
    @Test
    public void bothSideWithSameFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 5);
        Range result = ABSnotEqualValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), NotEqualValueRange.class);
        Assert.assertTrue(((NotEqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
    }

    // both sides has functions - same function
    // [ABS(value)!=5 & ABS(value)!=5]  ==> ABS(value)!=5
    @Test
    public void bothSideWithSameFunctions_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 7);
        Range result = ABSnotEqualValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has functions - different function
    // [ABS(value)!=5 & CEIL(value)!=5]  ==> composite range
    @Test
    public void bothSideWithDifferentFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.CEIL, 5);
        Range result = ABSnotEqualValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}