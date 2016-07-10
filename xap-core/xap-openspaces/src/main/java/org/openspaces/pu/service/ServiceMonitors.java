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

package org.openspaces.pu.service;

import java.io.Serializable;
import java.util.Map;

/**
 * A generic service that exists within a processing unit.
 *
 * @author kimchy
 * @see org.openspaces.pu.service.PlainServiceMonitors
 */
public interface ServiceMonitors extends Serializable {

    /**
     * Returns the id of the service monitor (usually the bean id).
     */
    String getId();

    /**
     * Returns the details of the service.
     *
     * <p>Note, should not be marshalled from the server to the client, the client should be able to
     * set it.
     */
    ServiceDetails getDetails();

    /**
     * Returns monitor values.
     */
    Map<String, Object> getMonitors();
}
