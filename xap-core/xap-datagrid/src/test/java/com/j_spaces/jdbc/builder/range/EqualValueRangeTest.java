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
import org.junit.Test;

import java.util.HashSet;

@com.gigaspaces.api.InternalApi
public class EqualValueRangeTest {

    @Test
    public void testSimpleEqual() {
        EqualValueRange range = new EqualValueRange("col", 5);

        Assert.assertEquals(5, range.getValue());
        Assert.assertEquals("col", range.getPath());
    }

    @Test
    public void testEqualAndEqual() {
        Range range1 = new EqualValueRange("col", 5);
        EqualValueRange range2 = new EqualValueRange("col", 5);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndEqual2() {
        Range range1 = new EqualValueRange("col", 5);
        EqualValueRange range2 = new EqualValueRange("col", 6);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndNotEquals() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new NotEqualValueRange("col", 7);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndNotEquals2() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new NotEqualValueRange("col", 5);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndGT() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", 1, true, null, true);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndGT2() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", 7, true, null, true);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndLT() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", null, true, 7, true);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndLT2() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", null, true, 3, true);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndRange() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", 1, true, 7, true);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndRange2() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new SegmentRange("col", 5, false, 8, true);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndIn() {
        Range range1 = new EqualValueRange("col", 5);
        HashSet<Integer> inValues = new HashSet<Integer>();
        inValues.add(1);
        inValues.add(3);
        inValues.add(5);
        inValues.add(10);
        Range range2 = new InRange("col", inValues);
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndIn2() {
        Range range1 = new EqualValueRange("col", 5);
        HashSet<Integer> inValues = new HashSet<Integer>();
        inValues.add(1);
        inValues.add(3);
        inValues.add(10);
        Range range2 = new InRange("col", inValues);
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndEmpty() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new EmptyRange();
        Range intersect = range1.intersection(range2);

        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndIsNull() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new IsNullRange("col");

        Range intersect = range1.intersection(range2);
        Assert.assertTrue(intersect instanceof EmptyRange);
    }

    @Test
    public void testEqualAndIsNotNull() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new NotNullRange("col");
        EqualValueRange intersect = (EqualValueRange) range1.intersection(range2);

        Assert.assertEquals(5, intersect.getValue());
        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndRegex() {
        Range range1 = new EqualValueRange("col", "a");
        Range range2 = new RegexRange("col", ".*aaa.*");
        CompositeRange intersect = (CompositeRange) range1.intersection(range2);

        Assert.assertEquals("col", intersect.getPath());
    }

    @Test
    public void testEqualAndNotRegex() {
        Range range1 = new EqualValueRange("col", 5);
        Range range2 = new NotRegexRange("col", ".*bbb.*");
        CompositeRange intersect = (CompositeRange) range1.intersection(range2);

        Assert.assertEquals("col", intersect.getPath());
    }
}
