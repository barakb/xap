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

package com.gigaspaces.server.filter;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.events.NotifyContext;
import com.gigaspaces.internal.utils.ClassUtils;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.FilterProvider;
import com.j_spaces.core.filters.ISpaceFilter;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;

/**
 * The filter is responsible to notify the backup space when all notification are sent to the user
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyAcknowledgeFilter extends FilterProvider implements ISpaceFilter {
    private static final long serialVersionUID = 1656923721022621158L;

    private final SpaceEngine _engine;

    /**
     * @param engine
     */
    public NotifyAcknowledgeFilter(SpaceEngine engine) {
        super();
        _engine = engine;

        setName(ClassUtils.getShortName(getClass()));
        setEnabled(true);
        setOpCodes(new int[]{FilterOperationCodes.AFTER_ALL_NOTIFY_TRIGGER});
        setActiveWhenBackup(false);
    }

    @Override
    public ISpaceFilter getFilter() {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.filters.ISpaceFilter#close()
     */
    public void close() throws RuntimeException {

    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.filters.ISpaceFilter#init(com.j_spaces.core.IJSpace, java.lang.String, java.lang.String, int)
     */
    public void init(IJSpace space, String filterId, String url, int priority)
            throws RuntimeException {
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.filters.ISpaceFilter#process(com.j_spaces.core.SpaceContext, com.j_spaces.core.filters.entry.ISpaceFilterEntry, int)
     */
    public void process(SpaceContext context, ISpaceFilterEntry entry,
                        int operationCode) throws RuntimeException {
        // send acknowledgment event to the backup
        NotifyContext notifyContext = ((NotifyEvent) entry).getNotifyContext();

        if (!notifyContext.isGuaranteedNotifications())
            return;

        OperationID opId = notifyContext.getOperationId();
        if (opId != null)
            _engine.getReplicationNode().outNotificationSentAndExecute(opId);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.filters.ISpaceFilter#process(com.j_spaces.core.SpaceContext, com.j_spaces.core.filters.entry.ISpaceFilterEntry[], int)
     */
    public void process(SpaceContext context, ISpaceFilterEntry[] entries,
                        int operationCode) throws RuntimeException {

    }

}
