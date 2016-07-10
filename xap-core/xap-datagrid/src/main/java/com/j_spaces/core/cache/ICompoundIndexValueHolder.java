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

package com.j_spaces.core.cache;

/**
 * Holder for compound index value (multi segments value)
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */

public interface ICompoundIndexValueHolder extends Comparable<ICompoundIndexValueHolder> {
    public static class LowEdge
            implements Comparable<Object> {
        @Override
        public int compareTo(Object o) {
            return o == this ? 0 : -1;
        }
    }

    public static class HighEdge
            implements Comparable<Object> {
        @Override
        public int compareTo(Object o) {
            return o == this ? 0 : 1;
        }
    }

    Object getValueBySegment(int segmentNumber);

    int getNumSegments();

    void setValueForSegment(Object value, int segmentNumber);

    Object[] getValues();
}
