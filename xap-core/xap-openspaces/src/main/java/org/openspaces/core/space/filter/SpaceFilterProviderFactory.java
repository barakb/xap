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


package org.openspaces.core.space.filter;

import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.FilterProvider;
import com.j_spaces.core.filters.ISpaceFilter;

/**
 * A {@link com.j_spaces.core.filters.FilterProvider FilterProvider} factory that accepts a concrete
 * {@link com.j_spaces.core.filters.ISpaceFilter ISpaceFilter} implementation in addition to all the
 * operation codes it will listen to.
 *
 * @author kimchy
 * @see com.j_spaces.core.filters.FilterProvider
 * @see com.j_spaces.core.filters.ISpaceFilter
 * @see com.j_spaces.core.filters.FilterOperationCodes
 */
public class SpaceFilterProviderFactory extends AbstractFilterProviderFactoryBean {

    private int[] operationCodes = new int[0];

    private String[] operationCodesNames = new String[0];

    /**
     * Returns a new filter provider based on the provided {@link #setFilter(Object)} and operation
     * codes.
     */
    @Override
    protected FilterProvider doGetFilterProvider() throws IllegalArgumentException {
        FilterProvider filterProvider = new FilterProvider(getBeanName(), (ISpaceFilter) getFilter());

        int[] actualOperationCodes = new int[operationCodes.length + operationCodesNames.length];
        System.arraycopy(operationCodes, 0, actualOperationCodes, 0, operationCodes.length);
        for (int i = 0; i < operationCodesNames.length; i++) {
            try {
                actualOperationCodes[operationCodes.length + i] =
                        FilterOperationCodes.class.getField(operationCodesNames[i].toUpperCase().replace('-', '_')).getInt(null);
            } catch (Exception e) {
                throw new IllegalArgumentException("No name found for [" + operationCodesNames[i] + "]");
            }
        }

        filterProvider.setOpCodes(actualOperationCodes);
        return filterProvider;
    }

    /**
     * Sets a list of the operation codes mapping to filter operations.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes
     */
    public void setOperationCodes(int[] operationCodes) {
        this.operationCodes = operationCodes;
    }

    /**
     * Sets the possible names for the given operation code.
     */
    public void setOperationCodesNames(String[] operationCodesNames) {
        this.operationCodesNames = operationCodesNames;
    }
}
