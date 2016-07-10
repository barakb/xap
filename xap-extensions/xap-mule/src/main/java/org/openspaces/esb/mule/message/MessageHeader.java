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

package org.openspaces.esb.mule.message;

/**
 * Base interface that expose mule meta data.
 *
 * <p> <B>Note:</B> implementation of this interface must have consistent results with equivalent
 * get/set Property method.
 *
 * @author yitzhaki
 */
public interface MessageHeader {

    /**
     * @param key   the key to be placed into this property list.
     * @param value the value corresponding to <tt>key</tt>.
     * @see #getProperty
     */
    void setProperty(String key, Object value);

    /**
     * @param key the property key.
     * @return the value in this property list with the specified key value.
     * @see #setProperty
     */
    Object getProperty(String key);


    /**
     * @return {@link java.util.Map} that contains all the properties.
     */
    MatchingMap<String, Object> getProperties();

    /**
     * Sets all the properties from the properties param.
     *
     * @param properties - properties to set.
     */
    void setProperties(MatchingMap<String, Object> properties);
}
