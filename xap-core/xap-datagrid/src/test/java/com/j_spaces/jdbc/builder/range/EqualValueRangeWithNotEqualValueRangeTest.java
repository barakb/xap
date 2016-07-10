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
public class EqualValueRangeWithNotEqualValueRangeTest {

    private EqualValueRange equalValueRange; // value = 5
    private EqualValueRange ABSequalValueRange; // ABS(value) = 5

    @Before
    public void setUp() throws Exception {
        equalValueRange = new EqualValueRange("col", 5);
        ABSequalValueRange = new EqualValueRange("col", FunctionUtils.ABS, 5);
    }

    // both ranges no have function
    // [value = 5 & value != 3 ]  ==> composite
    @Test
    public void noFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", 3);
        Range result = equalValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both ranges no have function
    // [value = 5 & value != 5 ]  ==> empty_range
    @Test
    public void noFunctions_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", 5);
        Range result = equalValueRange.intersection(nevr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // EqualRange with function
    // [ABS(value) = 5 & value != 5 ]  ==> composite range
    @Test
    public void equalWithFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", 5);
        Range result = ABSequalValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // NotEqualRange with function
    // [value = 5 & ABS(value) != 5 ]  ==> composite range
    @Test
    public void notEqualWithFunction() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 5);
        Range result = equalValueRange.intersection(nevr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // NotEqualRange with function
    // [value = 5 && ABS(value) != 3 ]  ==> composite range
    @Test
    public void notEqualWithFunction_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 3);
        Range result = equalValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
    }

    // both sides with same functions
    // [ABS(value) = 5 & ABS(value) != 5 ]  ==> composite range
    @Test
    public void bothSideWithSameFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 5);
        Range result = ABSequalValueRange.intersection(nevr);

        Assert.assertTrue(result.isEmptyRange());
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }

    // both sides with same functions
    // [ABS(value) = 5 & ABS(value) != 3 ]  ==> composite range
    @Test
    public void bothSideWithSameFunctions_1() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.ABS, 3);
        Range result = ABSequalValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), EqualValueRange.class);
        Assert.assertTrue(((EqualValueRange) result).getValue().equals(5));
        Assert.assertTrue(result.getFunctionCallDescription().getName().equals("ABS"));
    }

    // both sides with same functions
    // [ABS(value) = 5 & CEIL(value) != 3 ]  ==> composite range
    @Test
    public void bothSideWithDifferentFunctions() throws Exception {
        NotEqualValueRange nevr = new NotEqualValueRange("col", FunctionUtils.CEIL, 3);
        Range result = ABSequalValueRange.intersection(nevr);

        Assert.assertEquals(result.getClass(), CompositeRange.class);
        Assert.assertTrue(result.getFunctionCallDescription() == null);
    }


}
