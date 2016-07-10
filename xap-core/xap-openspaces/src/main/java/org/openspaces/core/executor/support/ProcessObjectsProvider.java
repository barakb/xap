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

package org.openspaces.core.executor.support;

/**
 * An interface used with {@link org.openspaces.core.executor.Task} allowing to return additional
 * objects to be processed on the node the task is executed. Processing an objects allows to inject
 * resources defined within the processing unit.
 *
 * @author kimchy
 */
public interface ProcessObjectsProvider {

    /**
     * Returns an array of objects that needs processing on the space node side. Processing an
     * objects allows to inject resources defined within the processing unit.
     */
    Object[] getObjectsToProcess();
}
