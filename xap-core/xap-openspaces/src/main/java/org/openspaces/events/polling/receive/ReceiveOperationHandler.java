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

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;

/**
 * Perform the actual receive operations for {@link org.openspaces.events.polling.SimplePollingEventListenerContainer}.
 * Can return either a single object or an array of objects.
 *
 * @author kimchy
 * @see org.openspaces.events.polling.SimplePollingEventListenerContainer
 */
public interface ReceiveOperationHandler {

    /**
     * Performs the actual receive operation. Return values allowed are single object or an array of
     * objects.
     *
     * @param template       The template to use for the receive operation.
     * @param gigaSpace      The GigaSpace interface to perform the receive operations with
     * @param receiveTimeout Receive timeout value
     * @return The receive result. <code>null</code> indicating no receive occurred. Single object
     * or an array of objects indicating the receive operation result.
     */
    Object receive(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException;
}
