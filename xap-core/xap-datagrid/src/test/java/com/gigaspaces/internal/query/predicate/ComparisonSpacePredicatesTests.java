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

package com.gigaspaces.internal.query.predicate;

import com.gigaspaces.internal.query.predicate.comparison.BetweenSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.ComparableScalarSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.EqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotNullSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotRegexSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NullSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.RegexSpacePredicate;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;

@com.gigaspaces.api.InternalApi
public class ComparisonSpacePredicatesTests {
    @Test
    public void testNull() {
        Assert.assertEquals(new NullSpacePredicate(), new NullSpacePredicate());
        Assert.assertNotEquals(new NullSpacePredicate(), null);
        Assert.assertNotEquals(null, new NullSpacePredicate());
        Assert.assertNotEquals(new NullSpacePredicate(), new NotNullSpacePredicate());

        testNull(null, true);
        testNull(new Object(), false);
    }

    private void testNull(Object value, boolean expectedResult) {
        NullSpacePredicate p = new NullSpacePredicate();
        Assert.assertEquals(expectedResult, p.execute(value));
        Assert.assertEquals("NULL()", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testNotNull() {
        Assert.assertEquals(new NotNullSpacePredicate(), new NotNullSpacePredicate());
        Assert.assertNotEquals(new NotNullSpacePredicate(), null);
        Assert.assertNotEquals(null, new NotNullSpacePredicate());
        Assert.assertNotEquals(new NotNullSpacePredicate(), new NullSpacePredicate());

        testNotNull(null, false);
        testNotNull(new Object(), true);
    }

    private void testNotNull(Object value, boolean expectedResult) {
        NotNullSpacePredicate p = new NotNullSpacePredicate();
        Assert.assertEquals(expectedResult, p.execute(value));
        Assert.assertEquals("NOTNULL()", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(new EqualsSpacePredicate(1), new EqualsSpacePredicate(1));
        Assert.assertNotEquals(new EqualsSpacePredicate(1), null);
        Assert.assertNotEquals(null, new EqualsSpacePredicate(1));
        Assert.assertNotEquals(new EqualsSpacePredicate(1), new NotEqualsSpacePredicate(1));
        Assert.assertNotEquals(new EqualsSpacePredicate(1), new EqualsSpacePredicate(2));

        testEquals("foo", "foo", true);
        testEquals("foo", "bar", false);

        testEquals(1, 1, true);
        testEquals(1, 2, false);

        Pojo a = new Pojo(1);
        Pojo b = new Pojo(2);
        Pojo c = new Pojo(2);

        testEquals(null, null, true);
        testEquals(null, a, false);
        testEquals(a, null, false);
        testEquals(a, a, true);
        testEquals(a, b, false);
        testEquals(b, a, false);
        testEquals(c, b, true);
        testEquals(b, c, true);
    }

    private void testEquals(Object value1, Object value2, boolean expectedResult) {
        EqualsSpacePredicate p = new EqualsSpacePredicate(value1);
        Assert.assertEquals(expectedResult, p.execute(value2));
        Assert.assertEquals("EQ(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testNotEquals() {
        Assert.assertEquals(new NotEqualsSpacePredicate(1), new NotEqualsSpacePredicate(1));
        Assert.assertNotEquals(new NotEqualsSpacePredicate(1), null);
        Assert.assertNotEquals(null, new NotEqualsSpacePredicate(1));
        Assert.assertNotEquals(new NotEqualsSpacePredicate(1), new EqualsSpacePredicate(1));
        Assert.assertNotEquals(new NotEqualsSpacePredicate(1), new NotEqualsSpacePredicate(2));

        testNotEquals("foo", "foo", false);
        testNotEquals("foo", "bar", true);
        testNotEquals(null, "foo", true);
        testNotEquals("foo", null, false);

        testNotEquals(1, 1, false);
        testNotEquals(1, 2, true);

        Pojo a = new Pojo(1);
        Pojo b = new Pojo(2);
        Pojo c = new Pojo(2);

        testNotEquals(null, null, false);
        testNotEquals(null, a, true);
        testNotEquals(a, null, false); //
        testNotEquals(a, a, false);
        testNotEquals(a, b, true);
        testNotEquals(b, a, true);
        testNotEquals(c, b, false);
        testNotEquals(b, c, false);
    }

    private void testNotEquals(Object value1, Object value2, boolean expectedResult) {
        NotEqualsSpacePredicate p = new NotEqualsSpacePredicate(value1);
        Assert.assertEquals(expectedResult, p.execute(value2));
        Assert.assertEquals("NEQ(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testGreater() {
        testGreaterIllegal(null);

        Assert.assertEquals(new GreaterSpacePredicate(1), new GreaterSpacePredicate(1));
        Assert.assertNotEquals(new GreaterSpacePredicate(1), null);
        Assert.assertNotEquals(null, new GreaterSpacePredicate(1));
        Assert.assertNotEquals(new GreaterSpacePredicate(1), new GreaterEqualsSpacePredicate(1));
        Assert.assertNotEquals(new GreaterSpacePredicate(1), new LessSpacePredicate(1));
        Assert.assertNotEquals(new GreaterSpacePredicate(1), new GreaterSpacePredicate(2));
        Assert.assertNotEquals(new GreaterSpacePredicate(1, null), new GreaterSpacePredicate(1, new StringLengthComparator()));

        testGreater(null, 1, false);

        testGreater(1, 1, false);
        testGreater(1, 2, false);
        testGreater(2, 1, true);

        Comparator<?> comparator = null;
        testGreater("aa", "b", comparator, false);
        testGreater("aa", "aa", comparator, false);
        testGreater("b", "aa", comparator, true);

        comparator = new StringLengthComparator();
        testGreater("aa", "b", comparator, true);
        testGreater("aa", "aa", comparator, false);
        testGreater("b", "aa", comparator, false);
    }

    private void testGreaterIllegal(Comparable<?> expectedValue) {
        try {
            new GreaterSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testGreater(Comparable<?> actualValue, Comparable<?> expectedValue, boolean expectedResult) {
        GreaterSpacePredicate p = new GreaterSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("GT(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    private void testGreater(Comparable<?> actualValue, Comparable<?> expectedValue, Comparator<?> comparator, boolean expectedResult) {
        GreaterSpacePredicate p = new GreaterSpacePredicate(expectedValue, comparator);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("GT(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testGreaterEquals() {
        testGreaterEqualsIllegal(null);

        Assert.assertEquals(new GreaterEqualsSpacePredicate(1), new GreaterEqualsSpacePredicate(1));
        Assert.assertNotEquals(new GreaterEqualsSpacePredicate(1), null);
        Assert.assertNotEquals(null, new GreaterEqualsSpacePredicate(1));
        Assert.assertNotEquals(new GreaterEqualsSpacePredicate(1), new GreaterSpacePredicate(1));
        Assert.assertNotEquals(new GreaterEqualsSpacePredicate(1), new LessEqualsSpacePredicate(1));
        Assert.assertNotEquals(new GreaterEqualsSpacePredicate(1), new GreaterEqualsSpacePredicate(2));
        Assert.assertNotEquals(new GreaterEqualsSpacePredicate(1, null), new GreaterEqualsSpacePredicate(1, new StringLengthComparator()));

        testGreaterEquals(null, 1, false);

        testGreaterEquals(1, 1, true);
        testGreaterEquals(1, 2, false);
        testGreaterEquals(2, 1, true);

        Comparator<?> comparator = null;
        testGreaterEquals("aa", "b", comparator, false);
        testGreaterEquals("aa", "aa", comparator, true);
        testGreaterEquals("b", "aa", comparator, true);

        comparator = new StringLengthComparator();
        testGreaterEquals("aa", "b", comparator, true);
        testGreaterEquals("aa", "aa", comparator, true);
        testGreaterEquals("b", "aa", comparator, false);
    }

    private void testGreaterEqualsIllegal(Comparable<?> expectedValue) {
        try {
            new GreaterEqualsSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testGreaterEquals(Comparable<?> actualValue, Comparable<?> expectedValue, boolean expectedResult) {
        GreaterEqualsSpacePredicate p = new GreaterEqualsSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("GE(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    private void testGreaterEquals(Comparable<?> actualValue, Comparable<?> expectedValue, Comparator<?> comparator, boolean expectedResult) {
        GreaterEqualsSpacePredicate p = new GreaterEqualsSpacePredicate(expectedValue, comparator);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("GE(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testLess() {
        testLessIllegal(null);

        Assert.assertEquals(new LessSpacePredicate(1), new LessSpacePredicate(1));
        Assert.assertNotEquals(new LessSpacePredicate(1), null);
        Assert.assertNotEquals(null, new LessSpacePredicate(1));
        Assert.assertNotEquals(new LessSpacePredicate(1), new LessEqualsSpacePredicate(1));
        Assert.assertNotEquals(new LessSpacePredicate(1), new GreaterSpacePredicate(1));
        Assert.assertNotEquals(new LessSpacePredicate(1), new LessSpacePredicate(2));
        Assert.assertNotEquals(new LessSpacePredicate(1, null), new LessSpacePredicate(1, new StringLengthComparator()));

        testLess(null, 1, false);

        testLess(1, 1, false);
        testLess(1, 2, true);
        testLess(2, 1, false);

        Comparator<?> comparator = null;
        testLess("aa", "b", comparator, true);
        testLess("aa", "aa", comparator, false);
        testLess("b", "aa", comparator, false);

        comparator = new StringLengthComparator();
        testLess("aa", "b", comparator, false);
        testLess("aa", "aa", comparator, false);
        testLess("b", "aa", comparator, true);
    }

    private void testLessIllegal(Comparable<?> expectedValue) {
        try {
            new LessSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testLess(Comparable<?> actualValue, Comparable<?> expectedValue, boolean expectedResult) {
        LessSpacePredicate p = new LessSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("LT(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    private void testLess(Comparable<?> actualValue, Comparable<?> expectedValue, Comparator<?> comparator, boolean expectedResult) {
        LessSpacePredicate p = new LessSpacePredicate(expectedValue, comparator);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("LT(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testLessEquals() {
        testLessEqualsIllegal(null);

        Assert.assertEquals(new LessEqualsSpacePredicate(1), new LessEqualsSpacePredicate(1));
        Assert.assertNotEquals(new LessEqualsSpacePredicate(1), null);
        Assert.assertNotEquals(null, new LessEqualsSpacePredicate(1));
        Assert.assertNotEquals(new LessEqualsSpacePredicate(1), new LessSpacePredicate(1));
        Assert.assertNotEquals(new LessEqualsSpacePredicate(1), new GreaterEqualsSpacePredicate(1));
        Assert.assertNotEquals(new LessEqualsSpacePredicate(1), new LessEqualsSpacePredicate(2));
        Assert.assertNotEquals(new LessEqualsSpacePredicate(1, null), new LessEqualsSpacePredicate(1, new StringLengthComparator()));

        testLessEquals(null, 1, false);

        testLessEquals(1, 1, true);
        testLessEquals(1, 2, true);
        testLessEquals(2, 1, false);

        Comparator<?> comparator = null;
        testLessEquals("aa", "b", comparator, true);
        testLessEquals("aa", "aa", comparator, true);
        testLessEquals("b", "aa", comparator, false);

        comparator = new StringLengthComparator();
        testLessEquals("aa", "b", comparator, false);
        testLessEquals("aa", "aa", comparator, true);
        testLessEquals("b", "aa", comparator, true);
    }

    private void testLessEqualsIllegal(Comparable<?> expectedValue) {
        try {
            new LessEqualsSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testLessEquals(Comparable<?> actualValue, Comparable<?> expectedValue, boolean expectedResult) {
        LessEqualsSpacePredicate p = new LessEqualsSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("LE(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    private void testLessEquals(Comparable<?> actualValue, Comparable<?> expectedValue, Comparator<?> comparator, boolean expectedResult) {
        LessEqualsSpacePredicate p = new LessEqualsSpacePredicate(expectedValue, comparator);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("LE(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testBetween() {
        BetweenSpacePredicate p;
        Comparator comparator = new StringLengthComparator();
        boolean lowInclusive = false;
        boolean highInclusive = false;

        p = new BetweenSpacePredicate(1, 2);
        testBetween(p, 1, 2, null, true, true);

        p = new BetweenSpacePredicate(1, 2, comparator);
        testBetween(p, 1, 2, comparator, true, true);

        p = new BetweenSpacePredicate(1, 2, lowInclusive, highInclusive);
        testBetween(p, 1, 2, null, lowInclusive, highInclusive);

        p = new BetweenSpacePredicate(1, 2, comparator, lowInclusive, highInclusive);
        testBetween(p, 1, 2, comparator, lowInclusive, highInclusive);
    }

    private void testBetween(BetweenSpacePredicate p, Comparable<?> low, Comparable<?> high,
                             Comparator<?> comparator, boolean lowInclusive, boolean highInclusive) {
        ComparableScalarSpacePredicate lowPredicate = p.getLowPredicate();
        Assert.assertSame(low, lowPredicate.getExpectedValue());
        Class<?> expectedLowClass = lowInclusive ? GreaterEqualsSpacePredicate.class : GreaterSpacePredicate.class;
        Assert.assertEquals(expectedLowClass, lowPredicate.getClass());

        ComparableScalarSpacePredicate highPredicate = p.getHighPredicate();
        Assert.assertSame(high, highPredicate.getExpectedValue());
        Class<?> expectedHighClass = highInclusive ? LessEqualsSpacePredicate.class : LessSpacePredicate.class;
        Assert.assertEquals(expectedHighClass, highPredicate.getClass());

        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testRegex() {
        testRegexIllegal(null);

        String regex1 = ".*aaa";
        Assert.assertEquals(new RegexSpacePredicate(regex1), new RegexSpacePredicate(regex1));
        Assert.assertNotEquals(new RegexSpacePredicate(regex1), null);
        Assert.assertNotEquals(null, new RegexSpacePredicate(regex1));
        Assert.assertNotEquals(new RegexSpacePredicate(regex1), new EqualsSpacePredicate(regex1));
        Assert.assertNotEquals(new RegexSpacePredicate(regex1), new LessSpacePredicate(regex1));

        String regex2 = "\\w";
        Assert.assertNotEquals(new RegexSpacePredicate(regex1), new RegexSpacePredicate(regex2));

        testRegex(null, regex1, false);

        testRegex("bgcaaa", regex1, true);
        testRegex("bcaaa", regex1, true);
        testRegex("bgcaa", regex1, false);
        testRegex("d", regex1, false);
        testRegex("bgcaaa", regex1, true);
        testRegex("bgcaaa", regex1, true);
    }

    private void testRegexIllegal(String expectedValue) {
        try {
            new RegexSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testRegex(Object actualValue, String expectedValue, boolean expectedResult) {
        RegexSpacePredicate p = new RegexSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("REGEX(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testNotRegex() {
        testNotRegexIllegal(null);

        String regex1 = ".*aaa";
        Assert.assertEquals(new NotRegexSpacePredicate(regex1), new NotRegexSpacePredicate(regex1));
        Assert.assertNotEquals(new NotRegexSpacePredicate(regex1), null);
        Assert.assertNotEquals(null, new NotRegexSpacePredicate(regex1));
        Assert.assertNotEquals(new NotRegexSpacePredicate(regex1), new EqualsSpacePredicate(regex1));
        Assert.assertNotEquals(new NotRegexSpacePredicate(regex1), new LessSpacePredicate(regex1));

        String regex2 = "\\w";
        Assert.assertNotEquals(new NotRegexSpacePredicate(regex1), new NotRegexSpacePredicate(regex2));

        testNotRegex(null, regex1, false);

        testNotRegex("bgcaaa", regex1, false);
        testNotRegex("bcaaa", regex1, false);
        testNotRegex("bgcaa", regex1, true);
        testNotRegex("d", regex1, true);
        testNotRegex("bgcaaa", regex1, false);
        testNotRegex("bgcaaa", regex1, false);
    }

    private void testNotRegexIllegal(String expectedValue) {
        try {
            new NotRegexSpacePredicate(expectedValue);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testNotRegex(Object actualValue, String expectedValue, boolean expectedResult) {
        NotRegexSpacePredicate p = new NotRegexSpacePredicate(expectedValue);
        Assert.assertEquals(expectedResult, p.execute(actualValue));
        Assert.assertEquals("REGEX(" + p.getExpectedValue() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }


    private static class Pojo implements Serializable {
        private static final long serialVersionUID = 1L;

        private int _val;

        public Pojo(int val) {
            this._val = val;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !this.getClass().equals(obj.getClass()))
                return false;
            Pojo other = (Pojo) obj;

            return this._val == other._val;
        }
    }

    private static class StringLengthComparator implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;

        public int compare(String s1, String s2) {
            return s1.length() - s2.length();
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && this.getClass().equals(obj.getClass());
        }
    }
}
