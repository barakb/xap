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


package com.gigaspaces.lrmi;

/**
 * Manages Gigaspaces LRMI layer
 *
 * @author eitany
 * @since 7.1
 */

public class LRMIManager {
    /**
     * Shutdown the LRMI runtime, once invoked the LRMI runtime is permanently destroyed and cannot
     * be reused in the current class loader which it was created in. As a result this operation is
     * irreversible.
     *
     * @since 7.1
     */
    public static void shutdown() {
        LRMIRuntime.getRuntime().shutdown();
    }
}
