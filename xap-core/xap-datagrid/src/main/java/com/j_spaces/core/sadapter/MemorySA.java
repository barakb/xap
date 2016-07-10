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

package com.j_spaces.core.sadapter;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.cache.context.Context;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Map;

/**
 * MemorySA is a mostly empty SA that is used for memory based space, it  "implements"
 * <code>IStorageAdapter</code> interface.
 *
 * The entries/templates are stored in the cache-manager
 */

// TODO DATASOURCE: remove MemorySA class.
@com.gigaspaces.api.InternalApi
public class MemorySA implements IStorageAdapter {
    public void initialize() throws SAException {
    }


    /**
     * Inserts a new entry to the SA storage.
     *
     * @param entryHolder entry to insert
     */
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate) throws SAException {
    }


    /**
     * updates an entry. <p/> <p/>
     *
     * @param updatedEntry new content, same UID and class
     */
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog,
                            boolean origin, boolean[] partialUpdateValuesIndicators) throws SAException {
    }

    /**
     * Gets an entry object from the storage adapter. <p/> <p/>
     *
     * @param uid       ID of the entry to get
     * @param classname class of the entry to get
     * @param template  selection template,may be null, currently used by cacheload/cachestore in
     *                  order to pass primery key fields when GS uid is not saved in an external DB
     * @return IEntryHolder
     */
    public IEntryHolder getEntry(Context context, Object uid, String classname, IEntryHolder template) throws SAException {
        return null;
    }


    /**
     * Removes an entry from the  SA storage. <p/>
     */
    public void removeEntry(Context context, IEntryHolder entryHolder, boolean origin,
                            boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {
    }


    /**
     * Returns an iterator with entries that match the template, and the other parameters. <p/>
     *
     * @param template    -    the entries should match that template.
     * @param SCNFilter   -   if != 0 gets only templates that are <= from SCNFilter.
     * @param leaseFilter if != 0 gets only templates that have expiration_time >= leaseFilter.
     * @param subClasses  -  array of sub classes of the template, the entries should belong to a
     *                    subcalss.
     * @return ISadapterIterator
     */
    public ISAdapterIterator makeEntriesIter(ITemplateHolder template,
                                             long SCNFilter,
                                             long leaseFilter,
                                             IServerTypeDesc[] subClasses) throws SAException {
        return null;
    }


    /**
     * Performs prepare to transaction- write to SA all new entries under the xtn, mark taken
     * entries under the xtn
     *
     * @param xtn               transaction to prepare
     * @param locked_entries    a vector of all entries locked (written or taken) under the xtn
     * @param singleParticipant - if true, a transactional SA needs to do all the transaction
     *                          related operations in one DB transaction and the prepare is actually
     *                          the commit (no other transaction related APIs will be called for
     *                          this xtn)
     */

    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> locked_entries,
                        boolean singleParticipant, Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException {
    }


    /**
     * Performs commit to transaction- delete taken entries, commit new entries.
     *
     * @param xtn        transaction to commit
     * @param anyUpdates true if any updates performed under tjis xtn
     */
    public void commit(ServerTransaction xtn, boolean anyUpdates) throws SAException {
    }

    /**
     * Performs rollback to transaction- rewrite taken entries, remove new entries.
     *
     * @param xtn        transaction to roll-back
     * @param anyUpdates true if any updates performed under tjis xtn
     */
    public void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException {
    }


    /**
     * Returns the number of entries that match the template.
     *
     * @param template   -    the entries should match the template.
     * @param subClasses -  subclassesof the template, the entries can be in one of them.
     * @return int - number of entries that match the parameters.
     */
    public int count(ITemplateHolder template,
                     String[] subClasses) throws SAException {
        return 0;
    }


    /**
     * called by the engine after all connections have been closed. <p/> if the ShotDownString
     * property is set- the string is executed by the SA
     */
    public void shutDown() throws SAException {
    }


    /* (non-Javadoc)
     * @see com.j_spaces.core.sadapter.IStorageAdapter#isReadOnly()
     */
    public boolean isReadOnly() {
        return false;
    }

    public boolean supportsExternalDB() {
        return false;
    }

    public boolean supportsPartialUpdate() {
        return true;
    }

    public ISAdapterIterator initialLoad(Context context, ITemplateHolder template)
            throws SAException {
        return null;
    }

    @Override
    public void introduceDataType(ITypeDesc typeDesc) {
    }

    @Override
    public void addIndexes(String typeName, SpaceIndex[] indexes) {
    }

    @Override
    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        return null;
    }

    @Override
    public Class<?> getDataClass() {
        return null;
    }

    @Override
    public Map<String, IEntryHolder> getEntries(Context context, Object[] ids, String typeName, IEntryHolder[] templates)
            throws SAException {
        return null;
    }

    @Override
    public boolean supportsGetEntries() {
        return false;
    }

}