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
import com.gigaspaces.internal.query.predicate.comparison.EqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.RegexSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AllSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AndSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AnySpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.NotSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.OrSpacePredicate;

import org.junit.Assert;
import org.junit.Test;

import static com.gigaspaces.internal.query.predicate.SpacePredicates.FALSE;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.TRUE;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.all;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.and;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.any;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.between;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.equal;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.greater;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.greaterEqual;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.in;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.less;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.lessEqual;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.not;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.notEqual;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.or;
import static com.gigaspaces.internal.query.predicate.SpacePredicates.regex;

@com.gigaspaces.api.InternalApi
public class SpacePredicatesTests {
    @Test
    public void testNot() {
        // Arrange:
        ISpacePredicate o = TRUE;

        // Act:
        NotSpacePredicate p = not(o);

        // Assert:
        Assert.assertSame(o, p.getOperand());
    }

    @Test
    public void testAnd() {
        // Arrange:
        ISpacePredicate o1 = TRUE;
        ISpacePredicate o2 = FALSE;

        // Act:
        AndSpacePredicate p = and(o1, o2);

        // Assert:
        Assert.assertSame(o1, p.getLeftOperand());
        Assert.assertSame(o2, p.getRightOperand());
    }

    @Test
    public void testOr() {
        // Arrange:
        ISpacePredicate o1 = TRUE;
        ISpacePredicate o2 = FALSE;

        // Act:
        OrSpacePredicate p = or(o1, o2);

        // Assert:
        Assert.assertSame(o1, p.getLeftOperand());
        Assert.assertSame(o2, p.getRightOperand());
    }

    @Test
    public void testAll() {
        // Arrange:
        ISpacePredicate o1 = TRUE;
        ISpacePredicate o2 = FALSE;

        // Act:
        AllSpacePredicate p = all(o1, o2);

        // Assert:
        Assert.assertEquals(2, p.getNumOfOperands());
        Assert.assertSame(o1, p.getOperand(0));
        Assert.assertSame(o2, p.getOperand(1));
    }

    @Test
    public void testAny() {
        // Arrange:
        ISpacePredicate o1 = TRUE;
        ISpacePredicate o2 = FALSE;

        // Act:
        AnySpacePredicate p = any(o1, o2);

        // Assert:
        Assert.assertEquals(2, p.getNumOfOperands());
        Assert.assertSame(o1, p.getOperand(0));
        Assert.assertSame(o2, p.getOperand(1));
    }

    @Test
    public void testEquals() {
        // Arrange:
        Object o = "foo";

        // Act:
        EqualsSpacePredicate p = equal(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testNotEquals() {
        // Arrange:
        Object o = "foo";

        // Act:
        NotEqualsSpacePredicate p = notEqual(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testGreater() {
        // Arrange:
        String o = "foo";

        // Act:
        GreaterSpacePredicate p = greater(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testGreaterEquals() {
        // Arrange:
        String o = "foo";

        // Act:
        GreaterEqualsSpacePredicate p = greaterEqual(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testLess() {
        // Arrange:
        String o = "foo";

        // Act:
        LessSpacePredicate p = less(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testLessEquals() {
        // Arrange:
        String o = "foo";

        // Act:
        LessEqualsSpacePredicate p = lessEqual(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }

    @Test
    public void testBetween() {
        // Arrange:
        String o1 = "foo";
        String o2 = "bar";

        // Act:
        BetweenSpacePredicate p = between(o1, o2);

        // Assert:
        Assert.assertSame(o1, p.getLowPredicate().getExpectedValue());
        Assert.assertSame(o2, p.getHighPredicate().getExpectedValue());
    }

    @Test
    public void testIn() {
        // Arrange:
        String o1 = "foo";
        String o2 = "bar";

        // Act:
        InSpacePredicate p = in(o1, o2);

        // Assert:
        Assert.assertEquals(2, p.getNumOfExpectedValues());
        Assert.assertTrue(p.getExpectedValues().contains(o1));
        Assert.assertTrue(p.getExpectedValues().contains(o2));
    }

    @Test
    public void testRegex() {
        // Arrange:
        String o = ".*foo.*";

        // Act:
        RegexSpacePredicate p = regex(o);

        // Assert:
        Assert.assertSame(o, p.getExpectedValue());
    }
}
