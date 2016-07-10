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
package net.jini.lookup;

/**
 * A delegate allowing for a proxy registered with the LUS to provide a version handle that will be
 * used to check if it is the same veresion as another service.
 *
 * <p>This allows, in the LookupCache, not to marshall proxies in order to check if they are of the
 * same version.
 *
 * @author kimchy
 */
public interface SameProxyVersionProvider {

    Object getVersion();
}
