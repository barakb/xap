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
public class DoubleHelper implements NumberHelper<Double> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public Double add(Number lhs, Number rhs) {
        return lhs.doubleValue() + rhs.doubleValue();
    }

    public Double sub(Number lhs, Number rhs) {
        return lhs.doubleValue() - rhs.doubleValue();
    }

    public Double mult(Number lhs, Number rhs) {
        return lhs.doubleValue() * rhs.doubleValue();
    }

    public Double div(Number lhs, Number rhs) {
        return lhs.doubleValue() / rhs.doubleValue();
    }

    public Double MAX_VALUE() {
        return Double.MAX_VALUE;
    }

    public Double MIN_VALUE() {
        return Double.MIN_VALUE;
    }

    public Double ONE() {
        return 1D;
    }

    public Double ZERO() {
        return 0D;
    }

    public Double cast(Number n) {
        if (n instanceof Double) {
            return (Double) n;
        }
        return n.doubleValue();
    }
}
