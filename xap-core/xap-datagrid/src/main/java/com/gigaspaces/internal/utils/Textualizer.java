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
 * @author Niv Ingberg
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class Textualizer {
    private final StringBuilder _sb;
    private boolean _hasProperties;

    public static String toString(Textualizable textualizable) {
        Textualizer textualizer = new Textualizer();
        textualizer.appendValue(textualizable);
        return textualizer._sb.toString();
    }

    private Textualizer() {
        this._sb = new StringBuilder();
    }

    public Textualizer appendValue(Object value) {
        if (value == null)
            _sb.append("null");
        else if (value instanceof Textualizable) {
            boolean hasProperties = _hasProperties;
            _hasProperties = false;
            _sb.append(value.getClass().getSimpleName());
            _sb.append('[');
            ((Textualizable) value).toText(this);
            _sb.append(']');
            _hasProperties = hasProperties;
        } else if (value instanceof Object[]) {
            Object[] array = (Object[]) value;
            _sb.append('{');
            if (array.length > 0) {
                _sb.append(array[0]);
                for (int i = 1; i < array.length; i++)
                    _sb.append(',').append(array[i]);
            }
            _sb.append('}');
        } else
            _sb.append(value);
        return this;
    }

    public Textualizer append(String name, Object value) {
        appendPropertiesSeparator();
        _sb.append(name);
        _sb.append('=');
        appendValue(value);
        return this;
    }

    public Textualizer appendIfNotNull(String name, Object value) {
        return value == null ? this : append(name, value);
    }

    private void appendPropertiesSeparator() {
        if (this._hasProperties)
            _sb.append(',');
        else
            this._hasProperties = true;
    }

    public Textualizer appendString(String str) {
        _sb.append(str);
        return this;
    }

    public Textualizer appendNewLine(String name, Object value) {
        _sb.append(StringUtils.NEW_LINE);
        _sb.append(name);
        _sb.append('=');
        appendValue(value);
        return this;
    }

    public void appendIterable(String name,
                               Iterable<?> iterable) {
        appendPropertiesSeparator();
        _sb.append(name);
        _sb.append("=[");
        for (Object object : iterable)
            appendValue(object);
        _sb.append(']');
    }
}
