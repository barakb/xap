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


import java.util.Iterator;

/**
 * implemention of all Mule metadata interfaces. This class should be extended by any Class that
 * wish to hold Mule metadata attributes.
 *
 * @author yitzhaki
 */
public abstract class AbstractMessageHeader implements CorrelationMessageHeader, ReplyToMessageHeader, UniqueIdMessageHeader {

    private MatchingMap<String, Object> properties = new MatchingMap<String, Object>();

    public Iterator<String> propertyNames() {
        return properties.keySet().iterator();
    }

    public MatchingMap<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(MatchingMap<String, Object> properties) {
        this.properties = properties;
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }


    public Object getReplyTo() {
        return properties.get(REPLY_TO);
    }

    public void setReplyTo(Object replyTo) {
        properties.put(REPLY_TO, replyTo);
    }


    public void setUniqueId(String id) {
        properties.put(UNIQUE_ID, id);
    }

    public String getUniqueId() {
        return (String) properties.get(UNIQUE_ID);
    }

    public String getCorrelationId() {
        return (String) properties.get(CORRELATION_ID);
    }

    public void setCorrelationId(String correlationId) {
        properties.put(CORRELATION_ID, correlationId);
    }

    public Integer getCorrelationSequence() {
        return (Integer) properties.get(CORRELATION_SEQUENCE);
    }

    public void setCorrelationSequence(Integer correlationSequence) {
        properties.put(CORRELATION_SEQUENCE, correlationSequence);
    }

    public Integer getCorrelationGroupSize() {
        return (Integer) properties.get(CORRELATION_GROUP_SIZE);
    }

    public void setCorrelationGroupSize(Integer correlationGroupSize) {
        properties.put(CORRELATION_GROUP_SIZE, correlationGroupSize);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractMessageHeader that = (AbstractMessageHeader) o;

        if (properties != null ? !properties.equals(that.properties) : that.properties != null)
            return false;

        return true;
    }

    public int hashCode() {
        return (properties != null ? properties.hashCode() : 0);
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
