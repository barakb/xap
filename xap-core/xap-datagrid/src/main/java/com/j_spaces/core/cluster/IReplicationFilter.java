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


package com.j_spaces.core.cluster;

import com.j_spaces.core.IJSpace;

/**
 * A replication filter is a special hook point inside the replication module. Two types of
 * replication filters can be defined - input replication and output replication. If the 2 classes
 * specified (for input and output) are the same, only one filter object will be used for both input
 * and output replication. <p> The filters are initiated when the replication module is initiated
 * (i.e. during space creation). In order to disable transmitted/received replicationFilterEntry
 * (replication entries) call discard on the filter entry.<br> <code>{@link
 * IReplicationFilterEntry#discard()}</code>
 * <pre><code>
 * // This snippet represents a default replication filter, for users using replication filters.
 * // The default filter simply prints the input and output packet codes.
 * public class DefaultReplicationFilter implements IReplicationFilter
 * {
 * 	private int	m_counter = 0;
 * 	final private IJspace m_space = null;
 * 	//...
 *
 * 	public void init(IJSpace space, String paramUrl, ReplicationPolicy replicationPolicy) {
 * 		m_space = space; //... initialize class variables
 *    }
 *
 * 	public void process(int direction, IReplicationFilterEntry replicationFilterEntry, String
 * remoteSpaceMemberName)
 *    {
 * 		String filterDirectionStr = null;
 * 		String operationCodeStr = null;
 * 		m_counter++;
 *
 * 		switch( direction )
 *      {
 * 			case IReplicationFilter.FILTER_DIRECTION_INPUT:	 filterDirectionStr = "INPUT"; break;
 * 			case IReplicationFilter.FILTER_DIRECTION_OUTPUT: filterDirectionStr = "OUTPUT"; break;
 *        }
 *
 * 		switch ( {@link IReplicationFilterEntry#getOperationType() replicationFilterEntry.getOperationType()}
 * )
 *      {
 * 			case {@link ReplicationOperationType#WRITE IReplicationFilterEntry.WRITE}: operationCodeStr =
 * "WRITE"; break;
 * 			case {@link ReplicationOperationType#TAKE IReplicationFilterEntry.TAKE}: ... break;
 * 			case {@link ReplicationOperationType#EXTEND_LEASE IReplicationFilterEntry.EXTEND_LEASE}: ...
 * break;
 * 			case {@link ReplicationOperationType#UPDATE IReplicationFilterEntry.UPDATE}: ... break;
 * 			case {@link ReplicationOperationType#DISCARD IReplicationFilterEntry.DISCARD}: ... break;
 * 			case {@link ReplicationOperationType#LEASE_EXPIRATION IReplicationFilterEntry.LEASE_EXPIRATION}:
 * ... break;
 *        }
 *
 * 		// we are simply print all replicated traffic.
 * 		System.out.println(
 * 			+ getClass().getName()
 * 			+ "| Space: " + m_space.getName()
 * 			+ " | Direction: "+ filterDirectionStr
 * 			+ " | Operation code: "+ operationCodeStr
 * 			+ " | Entry ("+ m_counter+"): "
 * 			+ {@link IReplicationFilterEntry#getClassName() replicationFilterEntry.getClassName()}
 * 			+ {@link IReplicationFilterEntry#getUID() replicationFilterEntry.getUID()}
 * 			+ {@link IReplicationFilterEntry#getFieldsValues() replicationFilterEntry.getFieldsValues()});
 *    }
 *  ...
 * }
 * </code></pre>
 *
 * @author Yechiel Fefer
 * @author Igor Goldenberg
 * @version 1.0
 * @see ReplicationFilterException
 */
public interface IReplicationFilter {
    /**
     * Output filter direction.
     *
     * @see IReplicationFilter#process(int, IReplicationFilterEntry, String)
     */
    public final static int FILTER_DIRECTION_OUTPUT = 1;

    /**
     * Input filter direction.
     *
     * @see IReplicationFilter#process(int, IReplicationFilterEntry, String)
     */
    public final static int FILTER_DIRECTION_INPUT = 2;

    /**
     * Initializes this filter.
     *
     * @param space             an embedded proxy to the space that contain this filter.
     * @param paramUrl          the url that was passed when this filter was created.
     * @param replicationPolicy replication policy for this replication group
     */
    void init(IJSpace space, String paramUrl, ReplicationPolicy replicationPolicy);

    /**
     * This method is called by sync/async replication when SyncPackets is about to get sent (for
     * output filter) or received (for input one). <br>The process method is called both when using
     * synchronous and asynchronous replication when ReplicationFilterEntry is about to be sent (for
     * output filter) or received (for input filter). When using transactions, this method is called
     * when calling the commit. When using asynchronous replication, the process method called is
     * based on the asynchronous replication parameters for all replication batch elements one by
     * one.
     *
     * @param direction              {@link #FILTER_DIRECTION_OUTPUT} or {@link #FILTER_DIRECTION_INPUT}
     * @param replicationFilterEntry replicated data
     * @param remoteSpaceMemberName  name of the remote peer space (name format is
     *                               container-name:space-name)
     */
    void process(int direction, IReplicationFilterEntry replicationFilterEntry, String remoteSpaceMemberName);

    /**
     * Closes this filter, enabling the developer to clean open resources.
     */
    void close();
}