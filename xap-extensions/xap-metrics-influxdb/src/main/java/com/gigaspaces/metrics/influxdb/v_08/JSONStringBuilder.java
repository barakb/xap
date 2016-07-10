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

package com.gigaspaces.metrics.influxdb.v_08;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public class JSONStringBuilder {
    private final StringBuilder json = new StringBuilder();

    @Override
    public String toString() {
        return json.toString();
    }

    public int length() {
        return json.length();
    }

    public void reset() {
        json.setLength(0);
    }

    public void appendTupleString(String key, String value) {
        appendString(key);
        appendTupleSeparator();
        appendString(value);
        json.append(',');
    }

    public void appendTupleArray(String key, Object[] value) {
        appendString(key);
        appendTupleSeparator();
        appendArray(value);
        json.append(',');
    }

    public void appendTupleList(String key, List value) {
        appendString(key);
        appendTupleSeparator();
        appendCollection(value);
        json.append(',');
    }

    public void appendValue(Object value) {
        if (value instanceof String)
            appendString((String) value);
        else if (value instanceof InfluxDBReporter.Point) {
            InfluxDBReporter.Point point = (InfluxDBReporter.Point) value;
            json.append('[');
            json.append(point.timestamp);
            json.append(',');
            appendValue(point.value);
            json.append(']');
        } else
            json.append(value);
    }

    public void appendString(String value) {
        json.append('"').append(value).append('"');
    }

    public void appendArray(Object[] array) {
        json.append('[');
        if (array.length != 0) {
            appendValue(array[0]);
            for (int i = 1; i < array.length; i++) {
                json.append(',');
                appendValue(array[i]);
            }
        }
        json.append(']');
    }

    public void appendCollection(Collection collection) {
        json.append('[');
        Iterator iterator = collection.iterator();
        if (iterator.hasNext()) {
            appendValue(iterator.next());
            while (iterator.hasNext()) {
                json.append(',');
                appendValue(iterator.next());
            }
        }
        json.append(']');
    }

    public void appendTupleSeparator() {
        json.append(':');
    }

    public void append(char c) {
        json.append(c);
    }

    public void setLength(int newLength) {
        json.setLength(newLength);
    }

    public void replaceLastChar(char c) {
        json.setCharAt(json.length() - 1, c);
    }
}
