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

import com.gigaspaces.cluster.activeelection.ICandidateEntry;

import net.jini.core.lookup.ServiceItem;

import java.util.List;


/**
 * This interface provides an acceptable decision for the proposed service candidate to
 * advance(aquire) the desired {@link ActiveElectionState.State} state.<br> The filter
 * implementation invokes by {@link ActiveElectionManager} to identify whether the managed service
 * candidate is acceptable to advance current state to <code>advanceState</code>.<br> The state will
 * be advanced by service <b>only</b> if {@link #isAcceptable(ActiveElectionState.State, List)}
 * returns <code>true</code>, otherwise the state stays without no change.<br>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see ActiveElectionManager
 * @see ActiveElectionState.State
 * @since 6.0
 **/
public interface IActiveElectionDecisionFilter {
    /**
     * Returns <code>true</code> if the managed service is acceptable to advance the state to
     * <code>advanceState</code>.
     *
     * @param advanceState The advance state (The state to aquire).
     * @param candidateSrv The service candidates to aquire <code>advanceState</code>.
     * @return <code>true</code> if the <code>advanceState</code> is acceptable by this filter.
     **/
    public boolean isAcceptable(ActiveElectionState.State advanceState, List<ServiceItem> candidateSrv);

    public int compareTo(ServiceItem service1, ServiceItem service2);

    public ICandidateEntry getCandidateEntry(ServiceItem service);
}
