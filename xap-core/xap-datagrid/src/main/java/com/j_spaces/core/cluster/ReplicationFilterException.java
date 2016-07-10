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

/**
 * This exception thrown back into the client when there is problem at the {@link
 * IReplicationFilter#process(int, IReplicationFilterEntry, String) IReplicationFilter.process()}
 * implementation.<br> For exception details call ReplicationFilterException.getCause().
 *
 * @author Igor Goldenberg
 * @version 4.0
 * @see com.j_spaces.core.cluster.IReplicationFilter
 **/

public class ReplicationFilterException
        extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private String _filterClassName;
    private String _sourceMemberName;
    private String _targetMemberName;
    private int _filterDirection; // input/output

    /**
     * Constructs an ReplicationFilterException with the specified detail message.
     *
     * @param message a <code>String</code> containing a detail message
     */
    public ReplicationFilterException(String message) {
        super(message);
    }

    /**
     * Constructs an ReplicationFilterException with the specified detail message and cause.
     *
     * @param message a <code>String</code> containing a detail message
     * @param cause   the cause
     */
    public ReplicationFilterException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an ReplicationFilterException with the specified detail message, source and target
     * member.
     *
     * @param message a <code>String</code> containing a detail message
     * @param cause   the cause
     */
    public ReplicationFilterException(String message, Throwable cause, String filterClassName,
                                      String sourceMemberName, String targetMemberName, int filterDirection) {
        super(message, cause);

        _filterClassName = filterClassName;
        _sourceMemberName = sourceMemberName;
        _targetMemberName = targetMemberName;
        _filterDirection = filterDirection;
    }

    /**
     * Gets the replication filter implementation class name.
     *
     * @return replication filter implementation class name.
     */
    public String getFilterClassName() {
        return _filterClassName;
    }

    /**
     * Gets the replication channel source member name.
     *
     * @return source member name in the following format: ContainerName:SpaceName
     */
    public String getSourceMemberName() {
        return _sourceMemberName;
    }

    /**
     * Gets the replication channel target member name.
     *
     * @return target member name in the following format: ContainerName:SpaceName
     */
    public String getTargetMemberName() {
        return _targetMemberName;
    }

    /**
     * Gets replication filter direction Input/Output.
     *
     * @return Can have of the following values {@link IReplicationFilter#FILTER_DIRECTION_INPUT
     * IReplicationFilter.FILTER_DIRECTION_INPUT} or {@link IReplicationFilter#FILTER_DIRECTION_OUTPUT
     * IReplicationFilter.FILTER_DIRECTION_OUTPUT}
     */
    public int getDirection() {
        return _filterDirection;
    }
}