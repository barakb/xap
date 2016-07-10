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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A factory allowing to create {@link org.openspaces.core.util.numbers.NumberHelper} implemenations
 * based on the provided {@link Number} type (such as <code>Integer</code> or <code>Float</code>).
 *
 * @author kimchy
 */
public class NumberHelperFactory {

    /**
     * A factory allowing to create {@link org.openspaces.core.util.numbers.NumberHelper}
     * implemenations based on the provided {@link Number} type (such as <code>Integer</code> or
     * <code>Float</code>).
     *
     * @param type The type to construct the mappaed {@link org.openspaces.core.util.numbers.NumberHelper}.
     * @return The {@link org.openspaces.core.util.numbers.NumberHelper} mapping for the provided
     * type
     * @throws IllegalArgumentException No {@link org.openspaces.core.util.numbers.NumberHelper} for
     *                                  the provided type
     */
    @SuppressWarnings("unchecked")
    public static <N extends Number> NumberHelper<N> getNumberHelper(Class<N> type) throws IllegalArgumentException {
        if (type.equals(Integer.class)) {
            return (NumberHelper<N>) new IntegerHelper();
        }

        if (type.equals(Long.class)) {
            return (NumberHelper<N>) new LongHelper();
        }

        if (type.equals(Float.class)) {
            return (NumberHelper<N>) new FloatHelper();
        }

        if (type.equals(Double.class)) {
            return (NumberHelper<N>) new DoubleHelper();
        }

        if (type.equals(Short.class)) {
            return (NumberHelper<N>) new ShortHelper();
        }

        if (type.equals(BigInteger.class)) {
            return (NumberHelper<N>) new BigIntegerHelper();
        }

        if (type.equals(BigDecimal.class)) {
            return (NumberHelper<N>) new BigDecimalHelper();
        }

        throw new IllegalArgumentException("No number helper support for type [" + type.getName() + "]");
    }
}
