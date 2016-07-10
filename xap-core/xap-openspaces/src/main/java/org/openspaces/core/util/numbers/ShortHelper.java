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
public class ShortHelper implements NumberHelper<Short> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public Short add(Number lhs, Number rhs) {
        return (short) (lhs.shortValue() + rhs.shortValue());
    }

    public Short sub(Number lhs, Number rhs) {
        return (short) (lhs.shortValue() - rhs.shortValue());
    }

    public Short mult(Number lhs, Number rhs) {
        return (short) (lhs.shortValue() * rhs.shortValue());
    }

    public Short div(Number lhs, Number rhs) {
        return (short) (lhs.shortValue() / rhs.shortValue());
    }

    public Short MAX_VALUE() {
        return Short.MAX_VALUE;
    }

    public Short MIN_VALUE() {
        return Short.MIN_VALUE;
    }

    public Short ONE() {
        return 1;
    }

    public Short ZERO() {
        return 0;
    }

    public Short cast(Number n) {
        if (n instanceof Short) {
            return (Short) n;
        }
        return n.shortValue();
    }
}
