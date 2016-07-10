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

import java.math.BigInteger;

/**
 * @author kimchy
 */
public class BigIntegerHelper implements NumberHelper<BigInteger> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public boolean isGreater(BigInteger lhs, BigInteger rhs) {
        return lhs.compareTo(rhs) > 0;
    }

    public BigInteger add(Number lhs, Number rhs) {
        BigInteger lhsBigInt = cast(lhs);
        BigInteger rhsBigInt = cast(rhs);
        return lhsBigInt.add(rhsBigInt);
    }

    public BigInteger sub(Number lhs, Number rhs) {
        BigInteger lhsBigInt = cast(lhs);
        BigInteger rhsBigInt = cast(rhs);
        return lhsBigInt.subtract(rhsBigInt);
    }

    public BigInteger mult(Number lhs, Number rhs) {
        BigInteger lhsBigInt = cast(lhs);
        BigInteger rhsBigInt = cast(rhs);
        return lhsBigInt.multiply(rhsBigInt);
    }

    public BigInteger div(Number lhs, Number rhs) {
        BigInteger lhsBigInt = cast(lhs);
        BigInteger rhsBigInt = cast(rhs);
        return lhsBigInt.divide(rhsBigInt);
    }

    public BigInteger MAX_VALUE() {
        return BigInteger.valueOf(Long.MAX_VALUE);
    }

    public BigInteger MIN_VALUE() {
        return BigInteger.valueOf(Long.MIN_VALUE);
    }

    public BigInteger ONE() {
        return BigInteger.ONE;
    }

    public BigInteger ZERO() {
        return BigInteger.ZERO;
    }

    public BigInteger cast(Number n) {
        if (n instanceof BigInteger) {
            return (BigInteger) n;
        }
        return BigInteger.valueOf(n.longValue());
    }
}
