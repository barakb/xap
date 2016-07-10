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
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * An exact value index scanner for compound index
 *
 * @author Yechiel
 * @since 9.5
 */

@com.gigaspaces.api.InternalApi
public class ExacValueCompoundIndexScanner extends ExactValueIndexScanner {
    private static final long serialVersionUID = 1L;

    private transient CompoundConvertedObjectWrapper _convertedValueWrapper;

    public ExacValueCompoundIndexScanner() {
        super();
    }

    public ExacValueCompoundIndexScanner(String indexName, Object[] segmentValues) {
        super(indexName, segmentValues);
        if (segmentValues.length <= 1)
            throw new InvalidParameterException();
    }

    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        // If converted value wrapper is not initialized, try to convert:
        Object[] segmentValues = (Object[]) getIndexValue();
        if (_convertedValueWrapper == null)
            _convertedValueWrapper = CompoundConvertedObjectWrapper.create(segmentValues, index);
        // If conversion could not be performed, return null
        if (_convertedValueWrapper == null)
            return null;

        IObjectsList res = index.getIndexEntries(_convertedValueWrapper.getValue());
        if (fifoGroupsScan && res != null)
            context.setFifoGroupIndexUsedInFifoGroupScan(res, index);
        return res;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return getIndexValue() != null;
    }

    /**
     * equality matching preserves fifo order
     */
    @Override
    public boolean supportsFifoOrder() {
        return true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

    }

    @Override
    public boolean supportsTemplateIndex() {
        return false;
    }

    public static ExacValueCompoundIndexScanner build(String name, List<Range> possibleSegments) {
        Object[] values = new Object[possibleSegments.size()];
        for (int i = 0; i < values.length; i++) {
            if (!possibleSegments.get(i).isEqualValueRange())
                return null;
            values[i] = ((EqualValueRange) (possibleSegments.get(i))).getValue();
            if (values[i] == null)
                return null;   //null segment not supported
        }

        return new ExacValueCompoundIndexScanner(name, values);
    }


}
