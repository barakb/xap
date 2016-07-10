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

package org.openspaces.core.internal.commons.math;


/**
 * Interface representing <a href="http://mathworld.wolfram.com/Field.html">field</a> elements.
 *
 * @param <T> the type of the field elements
 * @version $Revision: 811685 $ $Date: 2009-09-05 13:36:48 -0400 (Sat, 05 Sep 2009) $
 * @see Field
 * @since 2.0
 */
public interface FieldElement<T> {

    /**
     * Compute this + a.
     *
     * @param a element to add
     * @return a new element representing this + a
     */
    T add(T a);

    /**
     * Compute this - a.
     *
     * @param a element to subtract
     * @return a new element representing this - a
     */
    T subtract(T a);

    /**
     * Compute this &times; a.
     *
     * @param a element to multiply
     * @return a new element representing this &times; a
     */
    T multiply(T a);

    /**
     * Compute this &divide; a.
     *
     * @param a element to add
     * @return a new element representing this &divide; a
     * @throws ArithmeticException if a is the zero of the additive operation (i.e. additive
     *                             identity)
     */
    T divide(T a) throws ArithmeticException;

    /**
     * Get the {@link Field} to which the instance belongs.
     *
     * @return {@link Field} to which the instance belongs
     */
    Field<T> getField();

}
