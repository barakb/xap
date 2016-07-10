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
import com.gigaspaces.internal.query.predicate.comparison.NotNullSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NullSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.RegexSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AllSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AndSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AnySpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.NotSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.OrSpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.XorSpacePredicate;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPropertyGetter;
import com.gigaspaces.server.ServerEntry;

/**
 * Provides methods to easier usage of built-in space predicates.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class SpacePredicates {
    public static final FalseSpacePredicate FALSE = new FalseSpacePredicate();
    public static final TrueSpacePredicate TRUE = new TrueSpacePredicate();
    public static final NullSpacePredicate NULL = new NullSpacePredicate();
    public static final NotNullSpacePredicate NOT_NULL = new NotNullSpacePredicate();

    /**
     * Creates a NOT predicate wrapping the specified predicate.
     *
     * @param predicate Predicate to wrap.
     * @return A not predicate wrapping the specified predicate.
     */
    public static NotSpacePredicate not(ISpacePredicate predicate) {
        return new NotSpacePredicate(predicate);
    }

    /**
     * Creates an AND predicate wrapping the specified predicates.
     *
     * @param leftPredicate  Left operand of AND.
     * @param rightPredicate Right operand of AND.
     * @return AND predicate wrapping the specified predicates.
     */
    public static AndSpacePredicate and(ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return new AndSpacePredicate(leftPredicate, rightPredicate);
    }

    /**
     * Creates an OR predicate wrapping the specified predicates.
     *
     * @param leftPredicate  Left operand of OR.
     * @param rightPredicate Right operand of OR.
     * @return OR predicate wrapping the specified predicates.
     */
    public static OrSpacePredicate or(ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return new OrSpacePredicate(leftPredicate, rightPredicate);
    }

    /**
     * Creates an XOR predicate wrapping the specified predicates.
     *
     * @param leftPredicate  Left operand of XOR.
     * @param rightPredicate Right operand of XOR.
     * @return XOR predicate wrapping the specified predicates.
     */
    public static XorSpacePredicate xor(ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return new XorSpacePredicate(leftPredicate, rightPredicate);
    }

    /**
     * Creates an ALL predicate wrapping the specified predicates.
     *
     * @param predicates Predicates to wrap in ALL.
     * @return ALL predicate wrapping specified predicates.
     */
    public static AllSpacePredicate all(ISpacePredicate... predicates) {
        return new AllSpacePredicate(predicates);
    }

    /**
     * Creates an ANY predicate wrapping the specified predicates.
     *
     * @param predicates Predicates to wrap in ANY.
     * @return ANY predicate wrapping specified predicates.
     */
    public static AnySpacePredicate any(ISpacePredicate... predicates) {
        return new AnySpacePredicate(predicates);
    }

    /**
     * Creates an IS NULL predicate.
     *
     * @return IS NULL predicate.
     */
    public static NullSpacePredicate isNull() {
        return NULL;
    }

    /**
     * Creates an IS NOT NULL predicate.
     *
     * @return IS NOT NULL predicate.
     */
    public static NotNullSpacePredicate isNotNull() {
        return NOT_NULL;
    }

    /**
     * Creates an EQUALS predicate using the specified expected value.
     *
     * @param value Expected value for EQUALS predicate.
     * @return EQUALS predicate with specified expected value.
     */
    public static EqualsSpacePredicate equal(Object value) {
        return new EqualsSpacePredicate(value);
    }

    /**
     * Creates a NOT EQUALS predicate using the specified expected value.
     *
     * @param value Expected value for NOT EQUALS predicate.
     * @return NOT EQUALS predicate with specified expected value.
     */
    public static NotEqualsSpacePredicate notEqual(Object value) {
        return new NotEqualsSpacePredicate(value);
    }

    /**
     * Creates a GREATER predicate using the specified expected value.
     *
     * @param value Expected value for GREATER predicate.
     * @return GREATER predicate with specified expected value.
     */
    public static GreaterSpacePredicate greater(Comparable<?> value) {
        return new GreaterSpacePredicate(value);
    }

    /**
     * Creates a GREATER-EQUALS predicate using the specified expected value.
     *
     * @param value Expected value for GREATER-EQUALS predicate.
     * @return GREATER-EQUALS predicate with specified expected value.
     */
    public static GreaterEqualsSpacePredicate greaterEqual(Comparable<?> value) {
        return new GreaterEqualsSpacePredicate(value);
    }

    /**
     * Creates a LESS predicate using the specified expected value.
     *
     * @param value Expected value for LESS predicate.
     * @return LESS predicate with specified expected value.
     */
    public static LessSpacePredicate less(Comparable<?> value) {
        return new LessSpacePredicate(value);
    }

    /**
     * Creates a LESS-EQUALS predicate using the specified expected value.
     *
     * @param value Expected value for LESS-EQUALS predicate.
     * @return LESS-EQUALS predicate with specified expected value.
     */
    public static LessEqualsSpacePredicate lessEqual(Comparable<?> value) {
        return new LessEqualsSpacePredicate(value);
    }

    /**
     * Creates a BETWEEN predicate using the specified expected values.
     *
     * @param low  Expected low value for BETWEEN predicate.
     * @param high Expected high value for BETWEEN predicate.
     * @return BETWEEN predicate with specified expected values.
     */
    public static BetweenSpacePredicate between(Comparable<?> low, Comparable<?> high) {
        return new BetweenSpacePredicate(low, high);
    }

    /**
     * Creates an IN predicate using the specified expected values.
     *
     * @param values Expected values for IN predicate.
     * @return IN predicate with specified expected values.
     */
    public static InSpacePredicate in(Object... values) {
        return new InSpacePredicate(values);
    }

    /**
     * Creates a REGEX predicate using the specified expected value.
     *
     * @param pattern Expected value for REGEX predicate.
     * @return REGEX predicate with specified expected value.
     */
    public static RegexSpacePredicate regex(String pattern) {
        return new RegexSpacePredicate(pattern);
    }

    /**
     * Creates a predicate to get a property value and test it with another predicate.
     *
     * @param propertyName Property to test.
     * @param predicate    Predicate to apply on property.
     * @return Property predicate.
     */
    public static ValueGetterSpacePredicate<ServerEntry> property(String propertyName, ISpacePredicate predicate) {
        return new ValueGetterSpacePredicate<ServerEntry>(new SpaceEntryPropertyGetter(propertyName), predicate);
    }

    /**
     * Shortcut for property(propertyName, isNull()).
     *
     * @param propertyName Name of property.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyNull(String propertyName) {
        return property(propertyName, isNull());
    }

    /**
     * Shortcut for property(propertyName, isNotNull()).
     *
     * @param propertyName Name of property.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyNotNull(String propertyName) {
        return property(propertyName, isNotNull());
    }

    /**
     * Shortcut for property(propertyName, not(predicate)).
     *
     * @param propertyName Name of property.
     * @param predicate    predicate for not.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyNot(String propertyName, ISpacePredicate predicate) {
        return property(propertyName, not(predicate));
    }

    /**
     * Shortcut for property(propertyName, and(leftPredicate, rightPredicate)).
     *
     * @param propertyName   Name of property.
     * @param leftPredicate  leftPredicate for and.
     * @param rightPredicate rightPredicate for and.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyAnd(String propertyName, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return property(propertyName, and(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for property(propertyName, xor(leftPredicate, rightPredicate)).
     *
     * @param propertyName   Name of property.
     * @param leftPredicate  leftPredicate for xor.
     * @param rightPredicate rightPredicate for xor.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyXor(String propertyName, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return property(propertyName, xor(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for property(propertyName, or(leftPredicate, rightPredicate)).
     *
     * @param propertyName   Name of property.
     * @param leftPredicate  leftPredicate for or.
     * @param rightPredicate rightPredicate for or.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyOr(String propertyName, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return property(propertyName, or(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for property(propertyName, all(predicates)).
     *
     * @param propertyName Name of property.
     * @param predicates   predicates for all.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyAll(String propertyName, ISpacePredicate... predicates) {
        return property(propertyName, all(predicates));
    }

    /**
     * Shortcut for property(propertyName, any(predicates)).
     *
     * @param propertyName Name of property.
     * @param predicates   predicates for any.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyAny(String propertyName, ISpacePredicate... predicates) {
        return property(propertyName, any(predicates));
    }

    /**
     * Shortcut for property(propertyName, equal(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for equal.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyEqual(String propertyName, Object value) {
        return property(propertyName, equal(value));
    }

    /**
     * Shortcut for property(propertyName, notEqual(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for notEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyNotEqual(String propertyName, Object value) {
        return property(propertyName, notEqual(value));
    }

    /**
     * Shortcut for property(propertyName, greater(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for greater.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyGreater(String propertyName, Comparable<?> value) {
        return property(propertyName, greater(value));
    }

    /**
     * Shortcut for property(propertyName, greaterEqual(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for greaterEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyGreaterEqual(String propertyName, Comparable<?> value) {
        return property(propertyName, greaterEqual(value));
    }

    /**
     * Shortcut for property(propertyName, less(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for less.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyLess(String propertyName, Comparable<?> value) {
        return property(propertyName, less(value));
    }

    /**
     * Shortcut for property(propertyName, lessEqual(value)).
     *
     * @param propertyName Name of property.
     * @param value        value for lessEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyLessEqual(String propertyName, Comparable<?> value) {
        return property(propertyName, lessEqual(value));
    }

    /**
     * Shortcut for property(propertyName, between(low, high)).
     *
     * @param propertyName Name of property.
     * @param low          Low value for between.
     * @param high         High value for between.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyBetween(String propertyName, Comparable<?> low, Comparable<?> high) {
        return property(propertyName, between(low, high));
    }

    /**
     * Shortcut for property(propertyName, in(values)).
     *
     * @param propertyName Name of property.
     * @param values       Values for in.
     */
    public static ValueGetterSpacePredicate<ServerEntry> propertyIn(String propertyName, Object... values) {
        return property(propertyName, in(values));
    }

    /**
     * Creates a predicate to get a property value and test it with another predicate.
     *
     * @param path      Path to test.
     * @param predicate Predicate to apply on property.
     * @return Property predicate.
     */
    public static ValueGetterSpacePredicate<ServerEntry> path(String path, ISpacePredicate predicate) {
        return new ValueGetterSpacePredicate<ServerEntry>(new SpaceEntryPathGetter(path), predicate);
    }

    /**
     * Shortcut for path(path, isNull()).
     *
     * @param path Path to test.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathNull(String path) {
        return path(path, isNull());
    }

    /**
     * Shortcut for path(path, isNotNull()).
     *
     * @param path Path to test.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathNotNull(String path) {
        return path(path, isNotNull());
    }

    /**
     * Shortcut for path(path, not(predicate)).
     *
     * @param path      Path to test.
     * @param predicate predicate for not.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathNot(String path, ISpacePredicate predicate) {
        return path(path, not(predicate));
    }

    /**
     * Shortcut for path(path, and(leftPredicate, rightPredicate)).
     *
     * @param path           Path to test.
     * @param leftPredicate  leftPredicate for and.
     * @param rightPredicate rightPredicate for and.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathAnd(String path, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return path(path, and(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for path(path, or(leftPredicate, rightPredicate)).
     *
     * @param path           Path to test.
     * @param leftPredicate  leftPredicate for or.
     * @param rightPredicate rightPredicate for or.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathOr(String path, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return path(path, or(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for path(path, xor(leftPredicate, rightPredicate)).
     *
     * @param path           Path to test.
     * @param leftPredicate  leftPredicate for xor.
     * @param rightPredicate rightPredicate for xor.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathXor(String path, ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        return path(path, xor(leftPredicate, rightPredicate));
    }

    /**
     * Shortcut for path(path, all(predicates)).
     *
     * @param path       Path to test.
     * @param predicates predicates for all.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathAll(String path, ISpacePredicate... predicates) {
        return path(path, all(predicates));
    }

    /**
     * Shortcut for path(path, any(predicates)).
     *
     * @param path       Path to test.
     * @param predicates predicates for any.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathAny(String path, ISpacePredicate... predicates) {
        return path(path, any(predicates));
    }

    /**
     * Shortcut for path(path, equal(value)).
     *
     * @param path  Path to test.
     * @param value value for equal.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathEqual(String path, Object value) {
        return path(path, equal(value));
    }

    /**
     * Shortcut for path(path, notEqual(value)).
     *
     * @param path  Path to test.
     * @param value value for notEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathNotEqual(String path, Object value) {
        return path(path, notEqual(value));
    }

    /**
     * Shortcut for path(path, greater(value)).
     *
     * @param path  Path to test.
     * @param value value for greater.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathGreater(String path, Comparable<?> value) {
        return path(path, greater(value));
    }

    /**
     * Shortcut for path(path, greaterEqual(value)).
     *
     * @param path  Path to test.
     * @param value value for greaterEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathGreaterEqual(String path, Comparable<?> value) {
        return path(path, greaterEqual(value));
    }

    /**
     * Shortcut for path(path, less(value)).
     *
     * @param path  Path to test.
     * @param value value for less.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathLess(String path, Comparable<?> value) {
        return path(path, less(value));
    }

    /**
     * Shortcut for path(path, lessEqual(value)).
     *
     * @param path  Path to test.
     * @param value value for lessEqual.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathLessEqual(String path, Comparable<?> value) {
        return path(path, lessEqual(value));
    }

    /**
     * Shortcut for path(path, between(low, high)).
     *
     * @param path Path to test.
     * @param low  Low value for between.
     * @param high High value for between.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathBetween(String path, Comparable<?> low, Comparable<?> high) {
        return path(path, between(low, high));
    }

    /**
     * Shortcut for path(path, in(values)).
     *
     * @param path   Path to test.
     * @param values Values for in.
     */
    public static ValueGetterSpacePredicate<ServerEntry> pathIn(String path, Object... values) {
        return path(path, in(values));
    }
}
