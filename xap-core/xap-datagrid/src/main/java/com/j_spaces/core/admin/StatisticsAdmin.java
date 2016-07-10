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


package com.j_spaces.core.admin;

import com.j_spaces.core.exception.StatisticsNotAvailable;
import com.j_spaces.core.filters.StatisticsContext;
import com.j_spaces.core.filters.StatisticsHolder;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

/**
 * Provides a space statistics API.
 *
 * @author Michael Konnikov
 * @version 4.0
 */

public interface StatisticsAdmin extends Remote {
    /**
     * Checks if space statistics is available.
     *
     * @return <code>true</code> if space statistics is available
     * @throws RemoteException if a communication error occurs
     */
    public boolean isStatisticsAvailable() throws RemoteException;

    /**
     * Returns a string array contains statistics values for each operation. For example: WRITE
     * count: 10000, average time for last 2 seconds is 4293.1 READ count: 10000, average time for
     * last 2 seconds is 4293.1
     *
     * @return array of Strings that represents the space statistics
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     */
    public String[] getStatisticsStringArray()
            throws StatisticsNotAvailable, RemoteException;

    /**
     * Set statistics sampling rate.
     *
     * @param rate the rate to sample the space for statistics
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     */
    public void setStatisticsSamplingRate(long rate)
            throws StatisticsNotAvailable, RemoteException;

    /**
     * Checks statistics sampling rate.
     *
     * @return the statistics sampling rate
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     */
    public long getStatisticsSamplingRate()
            throws StatisticsNotAvailable, RemoteException;


    /**
     * Returns a {@link com.j_spaces.core.filters.StatisticsContext StatisticsContext} for specific
     * filter operation code.
     *
     * @param operationCode filter operation code defined in {@link com.j_spaces.core.filters.FilterOperationCodes
     *                      FilterOperationCodes}
     * @return StatisticsContext for the given operation code
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     * @see com.j_spaces.core.filters.FilterOperationCodes
     */
    public StatisticsContext getStatistics(int operationCode)
            throws StatisticsNotAvailable, RemoteException;

    /**
     * Returns statistics repository that contains a map of operation codes to {@link
     * com.j_spaces.core.filters.StatisticsContext StatisticsContext} objects for passed specific
     * filter operation codes.
     *
     * @param operationCodes filter array of operation codes defined in {@link
     *                       com.j_spaces.core.filters.FilterOperationCodes FilterOperationCodes}
     * @return map of operation codes to {@link com.j_spaces.core.filters.StatisticsContext
     * StatisticsContext}
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     */
    public Map<Integer, StatisticsContext> getStatistics(Integer[] operationCodes)
            throws StatisticsNotAvailable, RemoteException;

    /**
     * Returns statistics repository that contains a map of operation codes to {@link
     * com.j_spaces.core.filters.StatisticsContext StatisticsContext} objects for all filter
     * operation codes.
     *
     * @return map of operation codes to {@link com.j_spaces.core.filters.StatisticsContext
     * StatisticsContext}
     * @throws StatisticsNotAvailable when space statistics are not available
     * @throws RemoteException        if a communication error occurs
     */
    public Map<Integer, StatisticsContext> getStatistics() throws StatisticsNotAvailable, RemoteException;

    StatisticsHolder getHolder() throws RemoteException;
}
