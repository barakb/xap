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

/**
 * @author kimchy
 */
public class IntegerHelper implements NumberHelper<Integer> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public Integer add(Number lhs, Number rhs) {
        return lhs.intValue() + rhs.intValue();
    }

    public Integer sub(Number lhs, Number rhs) {
        return lhs.intValue() - rhs.intValue();
    }

    public Integer mult(Number lhs, Number rhs) {
        return lhs.intValue() * rhs.intValue();
    }

    public Integer div(Number lhs, Number rhs) {
        return lhs.intValue() / rhs.intValue();
    }

    public Integer MAX_VALUE() {
        return Integer.MAX_VALUE;
    }

    public Integer MIN_VALUE() {
        return Integer.MIN_VALUE;
    }

    public Integer ONE() {
        return 1;
    }

    public Integer ZERO() {
        return 0;
    }

    public Integer cast(Number n) {
        if (n instanceof Integer) {
            return (Integer) n;
        }
        return n.intValue();
    }
}
