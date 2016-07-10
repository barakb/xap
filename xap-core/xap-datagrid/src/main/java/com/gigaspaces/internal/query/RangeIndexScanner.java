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

import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Scans only the entries in the defined range
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class RangeIndexScanner extends AbstractQueryIndex {
    private static final long serialVersionUID = -5025380388944017192L;

    private Comparable<?> _min;
    private boolean _includeMin;
    private Comparable<?> _max;
    private boolean _includeMax;

    private transient short _minMatchCode;
    private transient ConvertedObjectWrapper _convertedMinWrapper;
    private transient ConvertedObjectWrapper _convertedMaxWrapper;

    public RangeIndexScanner() {
        super();
    }

    public RangeIndexScanner(String indexName, Comparable<?> min,
                             boolean includeMin, Comparable<?> max, boolean includeMax) {
        super(indexName);
        _min = min;
        _includeMin = includeMin;
        _max = max;
        _includeMax = includeMax;

        _minMatchCode = initialize();
    }

    private short initialize() {
        return _includeMin ? TemplateMatchCodes.GE : TemplateMatchCodes.GT;
    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        if (_convertedMinWrapper == null)
            _convertedMinWrapper = ConvertedObjectWrapper.create(_min, index.getValueType());
        if (_convertedMinWrapper == null)
            return null;

        if (_convertedMaxWrapper == null)
            _convertedMaxWrapper = ConvertedObjectWrapper.create(_max, index.getValueType());
        if (_convertedMaxWrapper == null)
            return null;

        return !fifoGroupsScan ? index.getExtendedIndexForScanning().establishScan(
                _convertedMinWrapper.getValue(), _minMatchCode, _convertedMaxWrapper.getValue(), _includeMax)
                : index.getExtendedFifoGroupsIndexForScanning().establishScan(
                _convertedMinWrapper.getValue(), _minMatchCode, _convertedMaxWrapper.getValue(), _includeMax);

    }

    @Override
    protected boolean hasIndexValue() {
        return _min != null || _max != null;
    }

    public Object getIndexValue() {
        return _min;
    }

    public boolean requiresOrderedIndex() {
        return true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _min = (Comparable<?>) in.readObject();
        _includeMin = in.readBoolean();
        _max = (Comparable<?>) in.readObject();
        _includeMax = in.readBoolean();

        _minMatchCode = initialize();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(_min);
        out.writeBoolean(_includeMin);
        out.writeObject(_max);
        out.writeBoolean(_includeMax);
    }

    public boolean supportsTemplateIndex() {
        return false;
    }
}
