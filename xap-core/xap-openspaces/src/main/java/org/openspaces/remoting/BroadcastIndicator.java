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


package org.openspaces.remoting;

/**
 * An interface optionally implemented by the first parameter in a remote invocation to indicate if
 * broadcasting is enabled or not.
 *
 * @author kimchy
 */
public interface BroadcastIndicator {

    /**
     * Return <code>true</code> if broadcast should be enabled or not. Return <code>null<code> if
     * should not affect it in any manner.
     */
    Boolean shouldBroadcast();

    /**
     * Returns the reducer that will be used in case broadcasting is enabled or not.
     */
    RemoteResultReducer getReducer();
}
