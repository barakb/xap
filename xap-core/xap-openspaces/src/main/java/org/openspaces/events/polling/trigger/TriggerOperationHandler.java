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


package org.openspaces.events.polling.trigger;

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;

/**
 * Allows to perform a trigger receive operation which control if the active receive operation will
 * be performed in a polling event container. This feature is mainly used when having polling event
 * operations with transactions where the trigger receive operation is performed outside of a
 * transaction thus reducing the creation of transactions did not perform the actual receive
 * operation.
 *
 * <p>If the {@link #triggerReceive(Object, org.openspaces.core.GigaSpace, long)} returns a non
 * <code>null</code> value, it means that the receive operation should take place. If it returns a
 * <code>null</code> value, no receive operation will be attempted.
 *
 * <p>A trigger operation handler can also control if the object returned from {@link
 * #triggerReceive(Object, org.openspaces.core.GigaSpace, long)} will be used as the template for
 * the receive operation by returning <code>true</code> in {@link #isUseTriggerAsTemplate()}. If
 * <code>false</code> is returned, the actual template configured in the polling event container
 * will be used.
 *
 * @author kimchy
 */
public interface TriggerOperationHandler {

    /**
     * Allows to perform a trigger receive operation which control if the active receive operation
     * will be performed in a polling event container. This feature is mainly used when having
     * polling event operations with transactions where the trigger receive operation is performed
     * outside of a transaction thus reducing the creation of transactions did not perform the
     * actual receive operation.
     *
     * <p>If this operation returns a non <code>null</code> value, it means that the receive
     * operation should take place. If it returns a <code>null</code> value, no receive operation
     * will be attempted.
     *
     * @param template       The template to use for the receive operation.
     * @param gigaSpace      The GigaSpace interface to perform the receive operations with
     * @param receiveTimeout Receive timeout value
     */
    Object triggerReceive(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException;

    /**
     * Controls if the object returned from {@link #triggerReceive(Object,
     * org.openspaces.core.GigaSpace, long)} will be used as the template for the receive operation
     * by returning <code>true</code>. If <code>false</code> is returned, the actual template
     * configured in the polling event container will be used.
     */
    boolean isUseTriggerAsTemplate();
}