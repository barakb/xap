/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.watch;

import org.jini.rio.core.ThresholdValues;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The WatchDataSource interface defines the semantics for a Watch to store Calculable records
 */
public interface WatchDataSource extends Remote {
    /**
     * Get the ID for the WatchDataSource
     */
    String getID() throws RemoteException;

    /**
     * Get the offset
     */
    int getOffset() throws RemoteException;

    /**
     * Set the maximum size for the Calculable history
     *
     * @param size The maximum size for the Calculable history
     */
    void setSize(int size) throws RemoteException;

    /**
     * Get the maximum size for the Calculable history
     *
     * @return The maximum size for the Calculable history
     */
    int getSize() throws RemoteException;

    /**
     * Clears the history
     */
    void clear() throws RemoteException;

    /**
     * Get the current size for the Caclulable history
     *
     * @return The current size for the Caclulable history
     */
    int getCurrentSize() throws RemoteException;

    /**
     * Add a Calculable record to the Calculable history
     *
     * @param calculable The Calculable record
     */
    void addCalculable(Calculable calculable) throws RemoteException;

    /**
     * Get all Calculable records from the Calculable history
     *
     * @return An array of Calculable records from the Calculable history. If there are no
     * Calculable records in the history, a zero-length array will be returned
     */
    Calculable[] getCalculable() throws RemoteException;

    /**
     * Get Calculable records from the Calculable history
     *
     * @param id The Watch identifier to match
     * @return Calculable[] An array of Calculable records from the Calculable history that match
     * the Watch id. If there are no Calculable records in the history that maych the Watch id, a
     * zero-length array will be returned
     */
    Calculable[] getCalculable(String id) throws RemoteException;

    /**
     * Get Calculable records from the Calculable history for the specified range
     *
     * @param offset The index of the first record to fetch
     * @param length The number of records to return
     * @return An array of Calculable records from the Calculable history within the provided range.
     * If there are no Calculable records in the range, a zero-length array will be returned
     */
    Calculable[] getCalculable(int offset, int length) throws RemoteException;

    /**
     * Get Calculable records from the Calculable history
     *
     * @param id     The Watch identifier to match
     * @param offset The index of the first record to match
     * @param length The number of records to compare
     * @return An array of Calculable records from the Calculable history that match the Watch id
     * within the provided range. If there are no Calculable records in the range that match the
     * Watch ID, a zero-length array will be returned
     */
    Calculable[] getCalculable(String id, int offset, int length)
            throws RemoteException;

    /**
     * Gets the last Calculable from the history
     *
     * @return The last Calculable in the history
     */
    Calculable getLastCalculable() throws RemoteException;

    /**
     * Gets the last Calculable from the history
     *
     * @param id The Watch identifier to match
     * @return The last Calculable in the history that matches the Watch ID
     */
    Calculable getLastCalculable(String id) throws RemoteException;

    /**
     * Getter for property thresholdValues.
     *
     * @return Value of property thresholdValues.
     */
    ThresholdValues getThresholdValues() throws RemoteException;

    /**
     * Set the ThresholdValues
     */
    void setThresholdValues(ThresholdValues tValues) throws RemoteException;

    /**
     * Closes the watch data source and unexports it from the runtime
     */
    void close() throws RemoteException;

    /**
     * Setter for property view.
     *
     * @param view The view class name, suitable for Class.forName
     */
    void setView(String view) throws RemoteException;

    /**
     * Getter for property view.
     *
     * @return The Value of the property view
     */
    String getView() throws RemoteException;
}
