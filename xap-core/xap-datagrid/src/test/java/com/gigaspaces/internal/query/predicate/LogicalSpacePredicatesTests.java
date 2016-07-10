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

import com.gigaspaces.internal.query.predicate.composite.AllSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AndSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AnySpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.NotSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.OrSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.XorSpacePredicate;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class LogicalSpacePredicatesTests {
    @Test
    public void testFalse() {
        Assert.assertEquals(new FalseSpacePredicate(), new FalseSpacePredicate());
        Assert.assertNotEquals(new FalseSpacePredicate(), null);
        Assert.assertNotEquals(null, new FalseSpacePredicate());
        Assert.assertNotEquals(new FalseSpacePredicate(), new TrueSpacePredicate());

        testFalse(null);
        testFalse(new Object());
    }

    private void testFalse(Object target) {
        ISpacePredicate p = new FalseSpacePredicate();

        Assert.assertEquals(false, p.execute(target));
        Assert.assertEquals("FALSE", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testTrue() {
        Assert.assertEquals(new TrueSpacePredicate(), new TrueSpacePredicate());
        Assert.assertNotEquals(new TrueSpacePredicate(), null);
        Assert.assertNotEquals(null, new TrueSpacePredicate());
        Assert.assertNotEquals(new TrueSpacePredicate(), new FalseSpacePredicate());

        testTrue(null);
        testTrue(new Object());
    }

    private void testTrue(Object target) {
        ISpacePredicate p = new TrueSpacePredicate();

        Assert.assertEquals(true, p.execute(target));
        Assert.assertEquals("TRUE", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testNot() {
        final ISpacePredicate t = SpacePredicates.TRUE;
        final ISpacePredicate f = SpacePredicates.FALSE;

        testNotIllegal(null);

        Assert.assertEquals(new NotSpacePredicate(t), new NotSpacePredicate(t));
        Assert.assertNotEquals(new NotSpacePredicate(t), null);
        Assert.assertNotEquals(null, new NotSpacePredicate(t));
        Assert.assertNotEquals(new NotSpacePredicate(t), new NotSpacePredicate(f));

        testNot(f, true);
        testNot(t, false);
    }

    private void testNotIllegal(ISpacePredicate operand) {
        try {
            new NotSpacePredicate(operand);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testNot(ISpacePredicate operand, boolean expected) {
        NotSpacePredicate p = new NotSpacePredicate(operand);

        Assert.assertEquals(expected, p.execute(null));
        Assert.assertEquals("NOT(" + p.getOperand().toString() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testAnd() {
        final ISpacePredicate f = SpacePredicates.FALSE;
        final ISpacePredicate t = SpacePredicates.TRUE;

        testAndIllegal(null, f);
        testAndIllegal(f, null);
        testAndIllegal(null, null);

        Assert.assertEquals(new AndSpacePredicate(t, t), new AndSpacePredicate(t, t));
        Assert.assertNotEquals(new AndSpacePredicate(t, t), null);
        Assert.assertNotEquals(null, new AndSpacePredicate(t, t));
        Assert.assertNotEquals(new AndSpacePredicate(t, t), new AndSpacePredicate(t, f));
        Assert.assertNotEquals(new AndSpacePredicate(t, t), new AndSpacePredicate(f, t));
        Assert.assertNotEquals(new AndSpacePredicate(t, t), new AndSpacePredicate(f, f));
        Assert.assertNotEquals(new AndSpacePredicate(t, t), new OrSpacePredicate(t, t));

        testAnd(f, f, false);
        testAnd(f, t, false);
        testAnd(t, f, false);
        testAnd(t, t, true);
    }

    private void testAndIllegal(ISpacePredicate left, ISpacePredicate right) {
        try {
            new AndSpacePredicate(left, right);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testAnd(ISpacePredicate left, ISpacePredicate right, boolean expected) {
        AndSpacePredicate p = new AndSpacePredicate(left, right);

        Assert.assertEquals(expected, p.execute(null));
        Assert.assertEquals("(" + p.getLeftOperand() + ") AND (" + p.getRightOperand() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testOr() {
        final ISpacePredicate f = SpacePredicates.FALSE;
        final ISpacePredicate t = SpacePredicates.TRUE;

        testOrIllegal(null, f);
        testOrIllegal(f, null);
        testOrIllegal(null, null);

        Assert.assertEquals(new OrSpacePredicate(t, t), new OrSpacePredicate(t, t));
        Assert.assertNotEquals(new OrSpacePredicate(t, t), null);
        Assert.assertNotEquals(null, new OrSpacePredicate(t, t));
        Assert.assertNotEquals(new OrSpacePredicate(t, t), new OrSpacePredicate(t, f));
        Assert.assertNotEquals(new OrSpacePredicate(t, t), new OrSpacePredicate(f, t));
        Assert.assertNotEquals(new OrSpacePredicate(t, t), new OrSpacePredicate(f, f));
        Assert.assertNotEquals(new OrSpacePredicate(t, t), new AndSpacePredicate(t, t));

        testOr(f, f, false);
        testOr(f, t, true);
        testOr(t, f, true);
        testOr(t, t, true);
    }

    private void testOrIllegal(ISpacePredicate left, ISpacePredicate right) {
        try {
            new OrSpacePredicate(left, right);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testOr(ISpacePredicate left, ISpacePredicate right, boolean expected) {
        OrSpacePredicate p = new OrSpacePredicate(left, right);

        Assert.assertEquals(expected, p.execute(null));
        Assert.assertEquals("(" + p.getLeftOperand() + ") OR (" + p.getRightOperand() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testXor() {
        final ISpacePredicate f = SpacePredicates.FALSE;
        final ISpacePredicate t = SpacePredicates.TRUE;

        testXorIllegal(null, f);
        testXorIllegal(f, null);
        testXorIllegal(null, null);

        Assert.assertEquals(new XorSpacePredicate(t, t), new XorSpacePredicate(t, t));
        Assert.assertNotEquals(new XorSpacePredicate(t, t), null);
        Assert.assertNotEquals(null, new XorSpacePredicate(t, t));
        Assert.assertNotEquals(new XorSpacePredicate(t, t), new XorSpacePredicate(t, f));
        Assert.assertNotEquals(new XorSpacePredicate(t, t), new XorSpacePredicate(f, t));
        Assert.assertNotEquals(new XorSpacePredicate(t, t), new XorSpacePredicate(f, f));
        Assert.assertNotEquals(new XorSpacePredicate(t, t), new OrSpacePredicate(t, t));

        testXor(f, f, false);
        testXor(f, t, true);
        testXor(t, f, true);
        testXor(t, t, false);
    }

    private void testXorIllegal(ISpacePredicate left, ISpacePredicate right) {
        try {
            new XorSpacePredicate(left, right);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testXor(ISpacePredicate left, ISpacePredicate right, boolean expected) {
        XorSpacePredicate p = new XorSpacePredicate(left, right);

        Assert.assertEquals(expected, p.execute(null));
        Assert.assertEquals("(" + p.getLeftOperand() + ") XOR (" + p.getRightOperand() + ")", p.toString());
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testAll() {
        final ISpacePredicate f = SpacePredicates.FALSE;
        final ISpacePredicate t = SpacePredicates.TRUE;

        ArrayList<ISpacePredicate> list = null;
        testAllIllegal(list);

        list = new ArrayList<ISpacePredicate>();
        list.add(null);
        testAllIllegal(list);

        testAllIllegal((ISpacePredicate) null);

        testAll(new ISpacePredicate[]{}, true);

        testAll(new ISpacePredicate[]{f}, false);
        testAll(new ISpacePredicate[]{t}, true);

        testAll(new ISpacePredicate[]{f, f}, false);
        testAll(new ISpacePredicate[]{f, t}, false);
        testAll(new ISpacePredicate[]{t, f}, false);
        testAll(new ISpacePredicate[]{t, t}, true);

        testAll(new ISpacePredicate[]{f, f, f}, false);
        testAll(new ISpacePredicate[]{f, f, t}, false);
        testAll(new ISpacePredicate[]{f, t, f}, false);
        testAll(new ISpacePredicate[]{f, t, t}, false);
        testAll(new ISpacePredicate[]{t, f, f}, false);
        testAll(new ISpacePredicate[]{t, f, t}, false);
        testAll(new ISpacePredicate[]{t, t, f}, false);
        testAll(new ISpacePredicate[]{t, t, t}, true);
    }

    private void testAllIllegal(List<ISpacePredicate> list) {
        try {
            new AllSpacePredicate(list);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testAllIllegal(ISpacePredicate... array) {
        try {
            new AllSpacePredicate(array);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testAll(ISpacePredicate[] predicates, boolean expected) {
        AllSpacePredicate p = new AllSpacePredicate(predicates);

        Assert.assertEquals(expected, p.execute(null));
        AssertEx.assertExternalizable(p);
    }

    @Test
    public void testAny() {
        final ISpacePredicate f = SpacePredicates.FALSE;
        final ISpacePredicate t = SpacePredicates.TRUE;

        ArrayList<ISpacePredicate> list = null;
        testAnyIllegal(list);

        list = new ArrayList<ISpacePredicate>();
        list.add(null);
        testAnyIllegal(list);

        testAnyIllegal((ISpacePredicate) null);

        testAny(new ISpacePredicate[]{}, false);

        testAny(new ISpacePredicate[]{f}, false);
        testAny(new ISpacePredicate[]{t}, true);

        testAny(new ISpacePredicate[]{f, f}, false);
        testAny(new ISpacePredicate[]{f, t}, true);
        testAny(new ISpacePredicate[]{t, f}, true);
        testAny(new ISpacePredicate[]{t, t}, true);

        testAny(new ISpacePredicate[]{f, f, f}, false);
        testAny(new ISpacePredicate[]{f, f, t}, true);
        testAny(new ISpacePredicate[]{f, t, f}, true);
        testAny(new ISpacePredicate[]{f, t, t}, true);
        testAny(new ISpacePredicate[]{t, f, f}, true);
        testAny(new ISpacePredicate[]{t, f, t}, true);
        testAny(new ISpacePredicate[]{t, t, f}, true);
        testAny(new ISpacePredicate[]{t, t, t}, true);
    }

    private void testAnyIllegal(List<ISpacePredicate> list) {
        try {
            new AnySpacePredicate(list);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testAnyIllegal(ISpacePredicate... array) {
        try {
            new AnySpacePredicate(array);
            Assert.fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Expected result.
        }
    }

    private void testAny(ISpacePredicate[] predicates, boolean expected) {
        AnySpacePredicate p = new AnySpacePredicate(predicates);

        Assert.assertEquals(expected, p.execute(null));
        AssertEx.assertExternalizable(p);
    }
}
