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


package org.openspaces.core.internal.commons.math.fraction;

import org.openspaces.core.internal.commons.math.Field;

import java.io.Serializable;

/**
 * Representation of the fractional numbers field. <p> This class is a singleton. </p>
 *
 * @version $Revision: 811827 $ $Date: 2009-09-06 11:32:50 -0400 (Sun, 06 Sep 2009) $
 * @see Fraction
 * @since 2.0
 */
public class FractionField implements Field<Fraction>, Serializable {

    /**
     * Serializable version identifier
     */
    private static final long serialVersionUID = -1257768487499119313L;

    /**
     * Private constructor for the singleton.
     */
    private FractionField() {
    }

    /**
     * Get the unique instance.
     *
     * @return the unique instance
     */
    public static FractionField getInstance() {
        return LazyHolder.INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    public Fraction getOne() {
        return Fraction.ONE;
    }

    /**
     * {@inheritDoc}
     */
    public Fraction getZero() {
        return Fraction.ZERO;
    }

    // CHECKSTYLE: stop HideUtilityClassConstructor

    /**
     * Holder for the instance. <p>We use here the Initialization On Demand Holder Idiom.</p>
     */
    private static class LazyHolder {
        /**
         * Cached field instance.
         */
        private static final FractionField INSTANCE = new FractionField();
    }
    // CHECKSTYLE: resume HideUtilityClassConstructor

    /**
     * Handle deserialization of the singleton.
     *
     * @return the singleton instance
     */
    private Object readResolve() {
        // return the singleton instance
        return LazyHolder.INSTANCE;
    }

}
