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
public class FloatHelper implements NumberHelper<Float> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public Float add(Number lhs, Number rhs) {
        return lhs.floatValue() + rhs.floatValue();
    }

    public Float sub(Number lhs, Number rhs) {
        return lhs.floatValue() - rhs.floatValue();
    }

    public Float mult(Number lhs, Number rhs) {
        return lhs.floatValue() * rhs.floatValue();
    }

    public Float div(Number lhs, Number rhs) {
        return lhs.floatValue() / rhs.floatValue();
    }

    public Float MAX_VALUE() {
        return Float.MAX_VALUE;
    }

    public Float MIN_VALUE() {
        return Float.MIN_VALUE;
    }

    public Float ONE() {
        return 1F;
    }

    public Float ZERO() {
        return 0F;
    }

    public Float cast(Number n) {
        if (n instanceof Float) {
            return (Float) n;
        }
        return n.floatValue();
    }
}
