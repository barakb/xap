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
public class EqualValueRangeWithEqualValueRangeTest {

    private EqualValueRange equalValueRange; // value = 5
    private EqualValueRange ABSEqualValueRange; // ABS(value) = 5

    @Before
    public void setUp() throws Exception {
        equalValueRange = new EqualValueRange("col", 5);
        ABSEqualValueRange = new EqualValueRange("col", FunctionUtils.ABS, 5);
    }

    // both ranges no have function
    // [value = 5 & value 3 ]  ==> empty_range
    @Test
    public void noFunctions() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", 3);
        Range result = equalValueRange.intersection(evr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges no have function
    // [value = 5 & value =5 ]  ==> value = 5
    @Test
    public void noFunctions_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", 5);
        Range result = equalValueRange.intersection(evr);

        Assert.assertTrue(result.isEqualValueRange());
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // left side has function
    // [ABS(value)=5 && value=-5 ]  ==> value = -5
    @Test
    public void leftSideHasFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", -5);
        Range result = ABSEqualValueRange.intersection(evr);

        Assert.assertTrue(result.isEqualValueRange());
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(-5));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // left side has function
    // [ABS(value)=5 && value=-6 ]  ==> empty_range
    @Test
    public void leftSideHasFunction_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", -6);
        Range result = ABSEqualValueRange.intersection(evr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // right side has function - equal to left side
    // [value=5 && ABS(value)=5]  ==> ABS(value) = 5
    @Test
    public void rightSideHasFunction() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", FunctionUtils.ABS, 5);
        Range result = equalValueRange.intersection(evr);

        Assert.assertTrue(result.isEqualValueRange());
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription().equals(FunctionUtils.ABS));
    }

    // both sides has functions - same function
    // [ABS(value)=5 && ABS(value)=7]  ==> empty_range
    @Test
    public void bothSideWithSameFunctions() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", FunctionUtils.ABS, 7);
        Range result = ABSEqualValueRange.intersection(evr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides has functions - same function
    // [ABS(value)=5 && ABS(value)=5]  ==> ABS(value)=5
    @Test
    public void bothSideWithSameFunctions_1() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", FunctionUtils.ABS, 5);
        Range result = ABSEqualValueRange.intersection(evr);

        Assert.assertTrue(result.isEqualValueRange());
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
    }

    // both sides has functions - different function
    // [ABS(value)=5 && CEIL(value)=5]  ==> composite range
    @Test
    public void bothSideWithDifferentFunctions() throws Exception {
        EqualValueRange evr = new EqualValueRange("col", FunctionUtils.CEIL, 5);
        Range result = ABSEqualValueRange.intersection(evr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }
}