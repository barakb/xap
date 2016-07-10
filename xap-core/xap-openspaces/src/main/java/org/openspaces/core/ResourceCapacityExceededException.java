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

package org.openspaces.core;

import org.springframework.dao.DataAccessException;

/**
 * This exception indicates that a resource usage on the server is exceeding its capacity
 *
 * {@link SpaceMemoryShortageException} {@link RedoLogCapacityExceededException} {@link
 * com.gigaspaces.client.ResourceCapacityExceededException}
 *
 * @author eitany
 * @since 7.1
 */
public class ResourceCapacityExceededException extends DataAccessException {

    private static final long serialVersionUID = -6944733039412790229L;

    public ResourceCapacityExceededException(com.gigaspaces.client.ResourceCapacityExceededException e) {
        super(e.getMessage(), e);
    }

    public ResourceCapacityExceededException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
