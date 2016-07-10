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
public class LongHelper implements NumberHelper<Long> {

    public int compare(Number lhs, Number rhs) {
        return cast(lhs).compareTo(cast(rhs));
    }

    public Long add(Number lhs, Number rhs) {
        return lhs.longValue() + rhs.longValue();
    }

    public Long sub(Number lhs, Number rhs) {
        return lhs.longValue() - rhs.longValue();
    }

    public Long mult(Number lhs, Number rhs) {
        return lhs.longValue() * rhs.longValue();
    }

    public Long div(Number lhs, Number rhs) {
        return lhs.longValue() / rhs.longValue();
    }

    public Long MAX_VALUE() {
        return Long.MAX_VALUE;
    }

    public Long MIN_VALUE() {
        return Long.MIN_VALUE;
    }

    public Long ONE() {
        return 1L;
    }

    public Long ZERO() {
        return 0L;
    }

    public Long cast(Number n) {
        if (n instanceof Long) {
            return (Long) n;
        }
        return n.longValue();
    }
}
