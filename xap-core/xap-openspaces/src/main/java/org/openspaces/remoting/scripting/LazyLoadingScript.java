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


package org.openspaces.remoting.scripting;

/**
 * Allows to define a lazy loading script which will not send the scipt contents during the
 * invocaiton if the scipt can be cached and compiled. If the scipt has already been compiled and
 * cached, it will be executed. If it was not, it will be loaded on the client and sent again for
 * execution.
 *
 * @author kimchy
 * @see org.openspaces.remoting.scripting.LazyLoadingRemoteInvocationAspect
 */
public interface LazyLoadingScript extends Script {

    /**
     * Returns <code>true</code> if the script has been loaded.
     */
    boolean hasScript();

    /**
     * Loads the script into memory.
     */
    void loadScript();
}
