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

package com.gigaspaces.cluster.activeelection.core;

/**
 * This interface provides notifications by {@link ActiveElectionManager} about new elected Active
 * service. The {@link ActiveElectionEvent#getActiveServiceItem()} provides the reference to the
 * Active service with all service attributes. <p> NOTE: The {@link IActiveElectionListener#onActive(ActiveElectionEvent)}
 * method should be executed as fast as possible to release the callback thread provided by {@link
 * ActiveElectionManager} <p> The following example shows how to check in listener implementation
 * whether the manage service is an ACTIVE:
 * <pre>
 *   <code>
 *     ActiveElectionManager _activeManager;
 *
 *     public void onActive( ActiveElectionEvent theEvent )
 *     {
 *       if ( _electManager.getState() == ActiveElectionState.State.ACTIVE )
 *         System.out.println("The manage service is an ACTIVE");
 *       else
 *         System.out.println("The manage service is an NOT an ACTIVE");
 *     }
 *   </code>
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see ActiveElectionEvent
 * @see ActiveElectionManager
 * @since 6.0
 **/
public interface IActiveElectionListener {

    /**
     * Notification about new elected Active service.
     *
     * @param theEvent The election event with an Active service reference.
     */
    public void onActive(ActiveElectionEvent theEvent);

    /**
     * Notification about split-brain. Identified more then 1 ACTIVE services. This method will be
     * called only after the split-brain was resolved.
     *
     * @param theEvent the active service (the new primary)
     */
    public void onSplitBrain(ActiveElectionEvent theEvent);

    /**
     * Notification about a backup that it's primary has changed due to split-brain.
     *
     * @param theEvent the active service (the new primary)
     */
    public void onSplitBrainBackup(ActiveElectionEvent theEvent);

    /**
     * Notification that split-brain was resolved - current service remains primary.
     *
     * @param theEvent the active service (same as the current elected)
     */
    public void onSplitBrainActive(ActiveElectionEvent theEvent);

    /**
     * Notification that an extra-backup was resolved. This method will be called on backup Spaces
     * that should terminate.
     *
     * @param theEvent the active service used as reference for resolution
     */
    public void onExtraBackup(ActiveElectionEvent theEvent);
}
