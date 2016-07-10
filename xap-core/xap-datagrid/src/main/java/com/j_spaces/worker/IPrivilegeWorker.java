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

package com.j_spaces.worker;

import com.j_spaces.core.IJSpace;

/**
 * @author Igor Goldenberg
 * @version 4.0
 * @deprecated This interface is reserved for internal usage only.
 **/
@Deprecated
public interface IPrivilegeWorker extends IWorker {
    /**
     * This method will be called on space initialization.
     *
     * @param proxy      Space proxy.
     * @param workerName Worker name.
     * @param arg        Any user defined argument.
     * @throws Exception Failed to initialize worker.
     **/
    public void init(IJSpace proxy, String workerName, String arg)
            throws Exception;
}
