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


package org.openspaces.core.util.numbers;

import java.util.Comparator;

/**
 * A generic interface on top of a specific {@link Number} implementation allowing to use it in a
 * generalized fashion.
 *
 * @author kimchy
 */
public interface NumberHelper<N extends Number> extends Comparator<Number> {

    /**
     * Casts the give {@link Number} into the type the number helper handles.
     */
    N cast(Number n);

    /**
     * Returns the maximum number for the specific type the number helper handles.
     */
    N MAX_VALUE();

    /**
     * Returns the minimum number for the specific type the number helper handles.
     */
    N MIN_VALUE();

    /**
     * Returns the "ONE" value for the given type.
     */
    N ONE();

    /**
     * Returns the "ZERO" value for the given type.
     */
    N ZERO();

    /**
     * Adds the two numbers (can be of any Number type) and returns the type result that the number
     * helper handles.
     */
    N add(Number lhs, Number rhs);

    /**
     * Substracts the two numbers (can be of any Number type) and returns the type result that the
     * number helper handles.
     */
    N sub(Number lhs, Number rhs);

    /**
     * Multiplies the two numbers (can be of any Number type) and returns the type result that the
     * number helper handles.
     */
    N mult(Number lhs, Number rhs);

    /**
     * Divides the two numbers (can be of any Number type) and returns the type result that the
     * number helper handles.
     */
    N div(Number lhs, Number rhs);
}
