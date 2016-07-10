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

package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.HashMap;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class PartialUpdateBulkDataItem
        extends BulkDataItem {
    private static final long serialVersionUID = 1L;

    private final HashMap<String, Object> _updatedValuesMap;

    public PartialUpdateBulkDataItem(IEntryHolder entryHolder,
                                     boolean[] partialUpdateValuesIndicators, ITypeDesc typeDesc, IDataConverter<IEntryPacket> converter) {
        super(entryHolder, typeDesc, BulkItem.PARTIAL_UPDATE, converter);

        // build the updated values map
        _updatedValuesMap = new HashMap<String, Object>();

        final IEntryData entryData = entryHolder.getEntryData();
        for (int i = 0; i < entryData.getFixedPropertiesValues().length; i++)
            if (!partialUpdateValuesIndicators[i])
                _updatedValuesMap.put(typeDesc.getFixedProperty(i).getName(), entryData.getFixedPropertyValue(i));
    }

    public Map<String, Object> getItemValues() {
        return _updatedValuesMap;
    }

    @Override
    public String toString() {
        return "BulkDataItem<Op: PARTIAL UPDATE, " + getTypeName() + "(" + getIdPropertyName() + ":" + getIdPropertyValue() + ")>";
    }


}
