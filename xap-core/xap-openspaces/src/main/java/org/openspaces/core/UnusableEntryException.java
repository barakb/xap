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

import org.springframework.dao.DataRetrievalFailureException;

/**
 * Thrown when one tries to get an Entry from a service, but the entry is unusable (due to
 * serialization or other errors). Wraps {@link net.jini.core.entry.UnusableEntryException}.
 *
 * @author kimchy
 */
public class UnusableEntryException extends DataRetrievalFailureException {

    private static final long serialVersionUID = -8652798659299131940L;

    public UnusableEntryException(String message, net.jini.core.entry.UnusableEntryException cause) {
        super(message, cause);
    }

    public UnusableEntryException(net.jini.core.entry.UnusableEntryException cause) {
        super(cause.getMessage(), cause);
    }

}
