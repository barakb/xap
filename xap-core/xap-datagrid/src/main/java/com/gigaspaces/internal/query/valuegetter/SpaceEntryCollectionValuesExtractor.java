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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortList;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Extracts collection values for the provided collection index path. Example: for
 * "list[*].property" returns all properties nested in each of the list's items.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryCollectionValuesExtractor extends AbstractSpaceValueGetter<ServerEntry> {
    private static final long serialVersionUID = 5423684060439741346L;

    private String _path;
    private ValuesExtractorParameters _parameters;

    /**
     * Default constructor required by Externalizable.
     */
    public SpaceEntryCollectionValuesExtractor() {
    }

    public SpaceEntryCollectionValuesExtractor(String path) {
        this._path = path;
    }

    @Override
    public Object getValue(ServerEntry target) {
        if (_parameters == null)
            _parameters = initializeValuesExtractorParameters();

        Object value = target.getPropertyValue(_parameters.getTokens()[0]);
        return (value == null) ? Collections.EMPTY_LIST : extractValues(value, 1, 0, new HashSet<Object>());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Collection<Object> extractValues(Object value, int tokenIndex, int currentCollectionIndex,
                                             Collection<Object> extractedValues) {
        // Get next collection
        while (tokenIndex != _parameters.getCollectionIndexes()[currentCollectionIndex]) {
            value = AbstractTypeIntrospector.getNestedValue(value, tokenIndex++, _parameters.getTokens(), _parameters.getPropertiesInfo(), _path);
            // NPE check
            if (value == null)
                return extractedValues;
        }

        // Collection validation
        if (!(value instanceof Collection<?>)) {
            if (value instanceof Object[])
                value = Arrays.asList((Object[]) value);
            else
                throw new IllegalArgumentException("value type [" + value.getClass().getName() + "] is not supported - expected Collection<?> or Object[].");
        }

        // If this is the last collection perform matching
        if (_parameters.getCollectionIndexes().length == ++currentCollectionIndex) {
            // If there is only one collection, return it (list[*])
            if (_parameters.getCollectionIndexes().length == 1 && tokenIndex == _parameters.getTokens().length)
                return (Collection<Object>) value;

            // list[*].items[*]
            if (tokenIndex == _parameters.getTokens().length) {
                extractedValues.addAll((Collection) value);
                return extractedValues;
            }

            // list[*].property
            for (Object item : (Collection<?>) value) {
                Object extractedValue = item;
                for (int i = tokenIndex; extractedValue != null && i < _parameters.getTokens().length; i++) {
                    extractedValue = AbstractTypeIntrospector.getNestedValue(extractedValue, i, _parameters.getTokens(), _parameters.getPropertiesInfo(), _path);
                }
                if (extractedValue == null)
                    continue;
                extractedValues.add(extractedValue);
            }

            // Otherwise, extract values for each of the collection's items
        } else {
            for (Object item : (Collection<?>) value) {
                if (item == null)
                    continue;
                extractValues(item, tokenIndex, currentCollectionIndex, extractedValues);
            }
        }

        return extractedValues;
    }

    private ValuesExtractorParameters initializeValuesExtractorParameters() {
        String[] temp = _path.split("\\.|\\[\\*\\]", -1);
        ArrayList<String> tokens = new ArrayList<String>();
        ShortList collectionIndexes = CollectionsFactory.getInstance().createShortList();
        short tokenIndex = 0;
        for (String token : temp) {
            if (token.length() == 0) {
                collectionIndexes.add(tokenIndex);
            } else {
                tokens.add(token);
                tokenIndex++;
            }
        }
        return new ValuesExtractorParameters(tokens, collectionIndexes.toNativeArray());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        SpaceEntryCollectionValuesExtractor other = (SpaceEntryCollectionValuesExtractor) obj;
        if (!ObjectUtils.equals(this._path, other._path))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _path = IOUtils.readString(in);
    }


    @Override
    protected void writeExternalImpl(ObjectOutput out) throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeString(out, _path);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        readExternalImpl(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    /**
     * A wrapper for values extractor needed parameters - we hold these in a wrapper object as final
     * fields for lazy initialization in {@link SpaceEntryCollectionValuesExtractor#getValue(ServerEntry)}
     * (newly created object is promised to have its final fields initialized & visible by other
     * threads)
     *
     * @author idan
     * @since 9.0.1
     */
    private static class ValuesExtractorParameters {
        private final String[] _tokens;
        private final short[] _collectionIndexes;
        private final SpacePropertyInfo[] _propertiesInfo;

        public ValuesExtractorParameters(ArrayList<String> tokens, short[] collectionIndexes) {
            String[] tokensArray = new String[tokens.size()];
            this._tokens = (String[]) tokens.toArray(tokensArray);
            this._collectionIndexes = collectionIndexes;
            this._propertiesInfo = new SpacePropertyInfo[tokens.size()];
        }

        public String[] getTokens() {
            return _tokens;
        }

        public short[] getCollectionIndexes() {
            return _collectionIndexes;
        }

        public SpacePropertyInfo[] getPropertiesInfo() {
            return _propertiesInfo;
        }

    }

}
