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

package com.gigaspaces.management;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.lrmi.LRMIRuntime;

/**
 * GigaSpaces Runtime
 *
 * @author eitany
 * @since 9.7
 */
@com.gigaspaces.api.InternalApi
public class GigaSpacesRuntime {
    /**
     * GigaSpaces creates static resources implicitly when creating GigaSpaces components, such as
     * remote/embedded space proxies. When using GigaSpaces outside of GigaSpaces containers (GSC)
     * and processing unit, in some scenarios this resources should be cleaned up when no longer
     * needed. This is relevant when hosting GigaSpaces components inside other application server
     * which create a new class loader per deployment, which loads GigaSpaces libraries into that
     * class loader each time. When this class loader is unloaded (Due to undeployment of the
     * application/service), this method should be called to cleanup the resources which were
     * created implicitly. This method shutdown GigaSpaces runtime, once invoked the GigaSpaces
     * runtime is destroyed and cannot be reused in the current class loader which it was created
     * in, as a result this is an irreversible process. This will only cleanup threads and resources
     * which are created implicitly when using GigaSpaces runtime, it will not clean up resources
     * the user has explicitly created such as space proxies, space instances and other components,
     * this components needs to be destroyed explicitly by the creator.
     *
     * This should only be used when using GigaSpaces runtime outside of GigaSpaces containers
     * (GSC), and only if GigaSpaces libraries are loaded into new class loader on each deployment
     * and not shared across multiple class loaders.
     *
     * @since 9.7
     */
    public static void shutdown() {
        LRMIRuntime.getRuntime().shutdown();
        ClassLoaderCache.getCache().shutdown();
    }
}
