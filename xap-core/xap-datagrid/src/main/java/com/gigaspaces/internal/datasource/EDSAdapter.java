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

package com.gigaspaces.internal.datasource;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.sync.SynchronizationStorageAdapter;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.sadapter.datasource.DataStorage;

import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_INHERITANCE_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_INHERITANCE_PROP;

/**
 * @author idan
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class EDSAdapter {

    public static IStorageAdapter create(SpaceEngine spaceEngine, JSpaceAttributes spaceAttributes) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        // try and get an actual instance provided through the properties
        Object dataSource = spaceAttributes.getCustomProperties().get(Constants.DataAdapter.DATA_SOURCE);
        if (dataSource == null) {
            // create it from the instnace itself
            String dataSourceClassName = spaceAttributes.getProperty(Constants.SPACE_CONFIG_PREFIX + DATA_SOURCE_CLASS_PROP,
                    DATA_SOURCE_CLASS_DEFAULT);
            dataSource = ClassLoaderHelper.loadClass(dataSourceClassName)
                    .newInstance();
        }

        final DataStorage<Object> dataStorage = new DataStorage<Object>(dataSource);

        final boolean supportsInheritance = Boolean.parseBoolean(spaceAttributes.getProperty(Constants.SPACE_CONFIG_PREFIX + SUPPORTS_INHERITANCE_PROP,
                SUPPORTS_INHERITANCE_DEFAULT));

        // The usage mode is needed in order to know whether to initialize the synchronization end point here or not
        // It is likely that on mirror EDS the mirror will be set with 'read-write' and the space will be set with 'read-only'
        final String usageString = spaceAttributes.getProperty(Constants.SPACE_CONFIG_PREFIX
                        + Constants.DataAdapter.USAGE,
                Constants.DataAdapter.USAGE_DEFAULT);
        boolean readonly = false;
        if (usageString.equalsIgnoreCase("read-only"))
            readonly = true;
        else if (usageString.equalsIgnoreCase("read-write"))
            readonly = false;
        else
            throw new IllegalArgumentException("usage space property only accepts the following values [read-only, read-write] but its value was set to: " + usageString);

        // If instance is not a persister - don't create a SynchronizationEndpointInterceptor (read-only)
        final EDSAdapterSynchronizationEndpoint synchronizationEndpointInterceptor = !readonly
                && (dataStorage.isDataPersister() || dataStorage.isBulkDataPersister()) ? new EDSAdapterSynchronizationEndpoint(spaceEngine,
                dataStorage)
                : null;
        return new SynchronizationStorageAdapter(spaceEngine,
                new EDSAdapterSpaceDataSource(dataStorage,
                        supportsInheritance),
                synchronizationEndpointInterceptor);
    }

}
