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

package com.j_spaces.core.client.version;

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.start.SystemBoot;
import com.j_spaces.kernel.ClassLoaderHelper;

import org.jini.rio.boot.CommonClassLoader;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the an Entry version for optimistic locking.
 *
 * @param <V> the type of the Object that holds the Version
 * @author Guy Korland
 * @version 1.0
 * @since 5.0EAG Build#1400-010
 */
@com.gigaspaces.api.InternalApi
public class VersionTable<V> {

    // Maps a each entry to its version
    final private ConcurrentHashMap<EntryInfoKey, V> _entryInfos;

    // collected weak references
    final protected ReferenceQueue<Object> _freeEntryQueue;

    private boolean _cleanerThreadInitialized = false;
    private EntryVersionCleaner<V> _entryVersionCleanerThread;

    /**
     * Creates a new Table and start a cleaner.
     */
    public VersionTable() {
        _entryInfos = new ConcurrentHashMap<EntryInfoKey, V>();
        _freeEntryQueue = new ReferenceQueue<Object>();
    }

    /**
     * Clear the the table
     */
    public void clear() {
        _entryInfos.clear();
    }

    /**
     * add EntryInfo to table.
     *
     * @param infoKey the entry key
     * @param version holds the entries version
     */
    protected void setEntryVersion(EntryInfoKey infoKey, V version) {
        initCleanerThreadIfNeeded();
        _entryInfos.put(infoKey, version);
    }

    /**
     * Init the cleaner thread only once if needed
     */
    private void initCleanerThreadIfNeeded() {
        if (_entryVersionCleanerThread == null) {
            synchronized (this) {
                if (!_cleanerThreadInitialized) {
                    // when running within the GSC, we need to make sure that the thread is started with the
                    // common Mapclass loader, so it won't keep a reference to the ServiceClassLoader
                    ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
                    if (SystemBoot.isRunningWithinGSC()) {
                        ClassLoaderHelper.setContextClassLoader(CommonClassLoader.getInstance(), true);
                    }
                    _entryVersionCleanerThread = new EntryVersionCleaner<V>(this.getClass().getSimpleName(), _freeEntryQueue, _entryInfos);
                    _entryVersionCleanerThread.start();
                    _cleanerThreadInitialized = true;
                    if (SystemBoot.isRunningWithinGSC()) {
                        ClassLoaderHelper.setContextClassLoader(origClassLoader, true);
                    }
                }
            }
        }

    }

    /**
     * return null; get entry version from table according to the value.
     *
     * @return the version object
     */
    protected V getEntryVersion(EntryInfoKey infoKey) {
        return _entryInfos.get(infoKey);
    }

    /**
     * Cleaning Thread, clean all the EntryInfos of the collected Objects.
     */
    final static private class EntryVersionCleaner<V> extends GSThread {
        final private ReferenceQueue<Object> _queue;
        final private Map<EntryInfoKey, V> _entryInfos;


        /**
         * Creates a new cleaner.
         *
         * @param queue      the queue that holds the entries to clean.
         * @param entryInfos the Map to clean.
         */
        public EntryVersionCleaner(String parentName, ReferenceQueue<Object> queue, Map<EntryInfoKey, V> entryInfos) {
            super(parentName + "$" + EntryVersionCleaner.class.getSimpleName());
            _queue = queue;
            _entryInfos = entryInfos;
            this.setDaemon(true);
        }

        /**
         * Block on the queue until an entry is been cleaned by the GC
         */
        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    Reference ref = _queue.remove(); // block op'
                    _entryInfos.remove(ref); // clean the _entryInfos table
                } catch (InterruptedException e) {

                    //Restore the interrupted status
                    interrupt();

                    //fall through
                    break;
                }
            }
        }
    }

    /*
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();

	   /* Closes this VersionTable by terminating its cleaner Thread and cleaning
        * all EntryInfos stored.
	    */

        synchronized (this) {
            if (_entryVersionCleanerThread != null)
                _entryVersionCleanerThread.interrupt();
            _cleanerThreadInitialized = false;
        }
        _entryInfos.clear();
    }
}
