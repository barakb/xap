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

package com.gigaspaces.internal.utils;

/**
 * A bounded string builder that does not safe checks and does not expand its capacity.
 *
 * @author kimchy
 */
public final class BoundedStringBuilder {

    private final char[] value;

    private int count;

    public BoundedStringBuilder(int size) {
        this.value = new char[size];
    }

    public BoundedStringBuilder append(String str) {
        if (str == null) str = "null";
        int len = str.length();
        if (len == 0) return this;
        str.getChars(0, len, value, count);
        count += len;
        return this;
    }

    public BoundedStringBuilder append(char c) {
        value[count++] = c;
        return this;
    }

    public BoundedStringBuilder append(char str[]) {
        System.arraycopy(str, 0, value, count, str.length);
        count += str.length;
        return this;
    }

    public void setLength(int length) {
        count = length;
    }

    public int length() {
        return count;
    }

    public String toString() {
        // Create a copy, don't share the array
        return new String(value, 0, count);
    }
}
