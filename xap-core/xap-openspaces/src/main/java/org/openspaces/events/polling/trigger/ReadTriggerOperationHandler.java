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
 * A trigger operation handler that performs read based on the provided template and returns its
 * result.
 *
 * @author kimchy
 */
public class ReadTriggerOperationHandler implements TriggerOperationHandler {

    private boolean useTriggerAsTemplate = false;

    /**
     * Controls if the object returned from {@link #triggerReceive(Object,
     * org.openspaces.core.GigaSpace, long)} will be used as the template for the receive operation
     * by returning <code>true</code>. If <code>false</code> is returned, the actual template
     * configured in the polling event container will be used.
     *
     * @see TriggerOperationHandler#isUseTriggerAsTemplate()
     */
    public void setUseTriggerAsTemplate(boolean useTriggerAsTemplate) {
        this.useTriggerAsTemplate = useTriggerAsTemplate;
    }

    /**
     * @see TriggerOperationHandler#isUseTriggerAsTemplate()
     */
    public boolean isUseTriggerAsTemplate() {
        return this.useTriggerAsTemplate;
    }

    /**
     * Uses {@link org.openspaces.core.GigaSpace#read(Object, long)} and returns its result.
     */
    public Object triggerReceive(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        return gigaSpace.read(template, receiveTimeout);
    }

    @Override
    public String toString() {
        return "Read Trigger, useTriggerAsTemplate[" + useTriggerAsTemplate + "]";
    }
}
