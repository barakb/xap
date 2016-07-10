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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Queries an entry that matches all the given properties. Properties are key-value pairs where key
 * is the entry fields name and the value if the field value.
 *
 * @author Anna Pavtulov
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class PropertiesQuery extends AbstractCustomQuery {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private Map<String, Object> _properties;

    /**
     * Default constructor for Externalizable.
     */
    public PropertiesQuery() {
    }

    public PropertiesQuery(Map<String, Object> properties, ITypeDesc typeDesc) {
        _properties = properties;

        for (Iterator<Entry<String, Object>> iter = _properties.entrySet().iterator(); iter.hasNext(); ) {
            Entry<String, Object> entry = iter.next();

            String propertyName = entry.getKey();
            Object indexValue = entry.getValue();

            //filter out null values
            if (indexValue == null) {
                iter.remove();
            } else {
                // path is the index name
                if (typeDesc.getIndexes().containsKey(propertyName))
                    addCustomIndex(new ExactValueIndexScanner(propertyName, indexValue));
            }
        }
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        for (Map.Entry<String, Object> property : _properties.entrySet()) {

            String propertyName = property.getKey();
            Object propertyValue = property.getValue();

            Object entryPropertyValue = entry.getPropertyValue(propertyName);

            if (!match(propertyValue, entryPropertyValue))
                return false;
        }
        return true;
    }

    protected boolean match(Object actual, Object expected) {
        if (actual == expected)
            return true;

        if (actual == null || expected == null)
            return false;

        return actual.equals(expected);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {

        List<Object> preparedValues = new LinkedList<Object>();
        StringBuilder b = new StringBuilder();

        for (Map.Entry<String, Object> property : _properties.entrySet()) {

            String propertyName = property.getKey();
            Object propertyValue = property.getValue();

            if (b.length() > 0)
                b.append(DefaultSQLQueryBuilder.AND);
            b.append(propertyName + " = ? ");
            preparedValues.add(propertyValue);
        }

        return new SQLQuery<Object>(typeDesc.getTypeName(), b.toString(),
                preparedValues.toArray());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        int length = in.readInt();
        if (length >= 0) {
            _properties = new HashMap<String, Object>(length);
            for (int i = 0; i < length; i++) {
                String key = IOUtils.readRepetitiveString(in);
                Object value = IOUtils.readObject(in);
                _properties.put(key, value);
            }
        }

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (_properties == null)
            out.writeInt(-1);
        else {
            int length = _properties.size();
            out.writeInt(length);
            for (Entry<String, Object> entry : _properties.entrySet()) {
                IOUtils.writeRepetitiveString(out, entry.getKey());
                IOUtils.writeObject(out, entry.getValue());
            }
        }
    }

}
