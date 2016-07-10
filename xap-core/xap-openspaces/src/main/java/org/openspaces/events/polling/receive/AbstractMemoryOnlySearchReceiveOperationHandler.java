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


package org.openspaces.events.polling.receive;

import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.TakeModifiers;

/**
 * Support class to perform receive operations with or without memory only search.. <p>If configured
 * to use Memory Only Search, the read/take operation will use {@link
 * ReadModifiers#MEMORY_ONLY_SEARCH} / {@link TakeModifiers#MEMORY_ONLY_SEARCH} accordingly.
 *
 * @author eitany
 * @since 9.5
 */
public abstract class AbstractMemoryOnlySearchReceiveOperationHandler extends AbstractNonBlockingReceiveOperationHandler {

    protected boolean useMemoryOnlySearch = false;


    /**
     * Returns whether the handler is configured to use memory only search or not.
     */
    public boolean isUseMemoryOnlySearch() {
        return useMemoryOnlySearch;
    }

    /**
     * Allows to configure the take/read operation to be performed in a memory only search manner.
     *
     * @param useMemoryOnlySearch if true, will use {@link ReadModifiers#MEMORY_ONLY_SEARCH} /
     *                            {@link TakeModifiers#MEMORY_ONLY_SEARCH} as part of the read/take
     *                            modifiers.
     */
    public void setUseMemoryOnlySearch(boolean useMemoryOnlySearch) {
        this.useMemoryOnlySearch = useMemoryOnlySearch;
    }


}
