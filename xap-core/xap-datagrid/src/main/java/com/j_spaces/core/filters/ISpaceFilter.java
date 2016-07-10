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


package com.j_spaces.core.filters;


import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;

/**
 * A filter is a special hook point inside the engine that enables integration with external systems
 * or implementation of user defined logic.
 *
 * The filter has a simple life-cycle: it initializes itself in the <code>init()</code> method,
 * processes events in its <code>process()</code> method and finally cleans resources on
 * <code>close()</code>.
 *
 * The <code>process()</code> method is called by the engine when an event that matches the filter's
 * Operation Code. The possible codes are specified by the <code>FilterOperationCodes</code> class.
 *
 * Filters are grouped by priorities. The priority should be between 0 and 4. Filters with higher
 * priorities are activated closer to the hook point. This means: <ul> <li>BEFORE filters - filters
 * with lower priorities will be activated first.</li> <li>AFTER filters - filters with higher
 * priorities will be activated first.</li> <li>ON_INIT filters - filters with lower priorities will
 * be activated first.</li> </ul>
 *
 * Since filters are activated in sensitive hook points in the engine, they should be careful with
 * performing long blocking calls, such as connecting to databases.
 *
 * @author Asaf Kariv
 * @version 1.0
 * @see FilterOperationCodes
 */
public interface ISpaceFilter {
    /**
     * Initializes this filter.
     *
     * @param space    an embedded proxy to the space that contain this filter.
     * @param filterId the name of this filter.
     * @param url      The URL that was passed when this filter was created.
     * @param priority defines the order in which filters are activated.
     * @throws java.lang.RuntimeException if this exception is raised, the filter will be discarded
     *                                    by the engine for the current engine execution ,unless
     *                                    defined otherwise in space configuration.
     */
    void init(IJSpace space, String filterId, String url, int priority) throws RuntimeException;

    /**
     * <pre>
     * This method is called by the engine when an event matching this filter's
     *  operation code occurs in the space engine.
     *
     * Only for SecurityFilter implementation: On SET_SECURITY operation code call
     * SpaceContext.setSecurityContext().
     * </pre>
     *
     * @param context       the Context passed by the caller, contains security context. some of the
     *                      filters (like ON_INIT) will always receive a null context.
     * @param entry         the event that occurred.
     * @param operationCode the operation that defines when this filter is activated. The operation
     *                      codes are defined in {@link FilterOperationCodes}.
     * @throws java.lang.RuntimeException if this exception is raised for BEFORE filters, it aborts
     *                                    the execution of the operation.
     */
    void process(SpaceContext context, ISpaceFilterEntry entry, int operationCode) throws RuntimeException;

    /**
     * This method is called by the engine when an event matching this filter's operation code
     * occurs in the engine.<p> Notice: This is a special case that is called only on {@link
     * IJSpace#update(net.jini.core.entry.Entry, net.jini.core.transaction.Transaction, long, long,
     * int) update()} only on {@link FilterOperationCodes#AFTER_UPDATE AFTER_UPDATE}.<br> Also on
     * {@link FilterOperationCodes#BEFORE_NOTIFY_TRIGGER BEFORE_NOTIFY_TRIGGER} and {@link
     * FilterOperationCodes#AFTER_NOTIFY_TRIGGER AFTER_NOTIFY_TRIGGER}.
     *
     * @param context       the Context passed by the caller, contains security context. some of the
     *                      filters (like ON_INIT) will always receive a null context.
     * @param entries       an array of two elements:<p> On {@link FilterOperationCodes#AFTER_UPDATE
     *                      AFTER_UPDATE}: the first element represents the old entry while the
     *                      second is the new entry.<p>
     *
     *                      On {@link FilterOperationCodes#BEFORE_NOTIFY_TRIGGER
     *                      BEFORE_NOTIFY_TRIGGER} and {@link FilterOperationCodes#AFTER_NOTIFY_TRIGGER
     *                      AFTER_NOTIFY_TRIGGER}: the first element represents the entry while the
     *                      second is the template.
     * @param operationCode the operation that defines when this filter is activated. The operation
     *                      codes are defined in {@link FilterOperationCodes}.
     * @throws java.lang.RuntimeException if this exception is raised for BEFORE filters,
     */
    void process(SpaceContext context, ISpaceFilterEntry[] entries, int operationCode) throws RuntimeException;

    /**
     * Closes this filter, enabling the developer to clean open resources.
     *
     * @throws java.lang.RuntimeException if this exception is raised it will be logged. This should
     *                                    be used as away to report a filter internal error.
     */
    void close() throws RuntimeException;


}
