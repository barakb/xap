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

/*
 * @(#)VersionTableTest.java   15/01/2008
 *
 * Copyright 2008 GigaSpaces Technologies Inc.
 */
package com.j_spaces.core.client.version.space;

import com.j_spaces.core.client.EntryInfo;

import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.Random;

/**
 * Test the VersionTable.
 *
 * @author Guy
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class VersionTableTests {
    /**
     * Make sure that a single insert and get returns the same info.
     */
    @Test
    public void testVersionInsert() throws Exception {
        SpaceVersionTable table = new SpaceVersionTable();
        EntryInfo info = new EntryInfo("uid1", 1);
        MyEntry entry = new MyEntry();
        table.setEntryVersion(entry, info);
        Assert.assertEquals(table.getEntryVersion(entry), info);
    }

    /**
     * Make sure that a multi inserts and gets returns the same info for each entry.
     */
    @Test
    public void testMultiVersionInsert() throws Exception {
        SpaceVersionTable table = new SpaceVersionTable();
        MyEntry[] entries = new MyEntry[100];
        EntryInfo[] infos = new EntryInfo[entries.length];
        for (int i = 0; i < entries.length; ++i) {
            entries[i] = new MyEntry();
            infos[i] = new EntryInfo(Integer.toString(i), i);
            table.setEntryVersion(entries[i], infos[i]);
        }

        for (int i = 0; i < entries.length; ++i)
            Assert.assertEquals(table.getEntryVersion(entries[i]), infos[i]);
    }

    /**
     * Make sure that a multi inserts are cleaned by the GC if the entry isn't held outside the
     * table.
     */
    @Test
    public void testMultiVersionInsertAndCleaned() throws Exception {
        SpaceVersionTable table = new SpaceVersionTable();
        MyEntry[] entries = new MyEntry[100];
        EntryInfo[] infos = new EntryInfo[entries.length];
        for (int i = 0; i < entries.length; ++i) {
            entries[i] = new MyEntry();
            infos[i] = new EntryInfo(Integer.toString(i), i);
            table.setEntryVersion(entries[i], infos[i]);
        }

        WeakReference<MyEntry>[] weakntries = new WeakReference[entries.length];
        WeakReference<EntryInfo>[] weakinfos = new WeakReference[entries.length];

        for (int i = 0; i < entries.length; ++i) {
            Assert.assertNotNull(entries[i]);
            weakntries[i] = new WeakReference<MyEntry>(entries[i]);
            entries[i] = null;
            Assert.assertNotNull(infos[i]);
            weakinfos[i] = new WeakReference<EntryInfo>(infos[i]);
            infos[i] = null;
        }

        runGC(); // collect the entries
        Thread.sleep(1000); // let the Cleaner clean the infos
        runGC(); // collect the infos

        for (int i = 0; i < entries.length; ++i) {
            Assert.assertNull(weakntries[i].get());
            Assert.assertNull(weakinfos[i].get());
        }
    }

    private Runtime _rt = Runtime.getRuntime();

    private long usedMemory() {
        return _rt.totalMemory() - _rt.freeMemory();
    }

    private void runGC() {
        long usedMem1 = usedMemory(), usedMem2 = Long.MAX_VALUE;

        for (int i = 0; (usedMem1 < usedMem2) && (i < 1000); ++i) {
            _rt.runFinalization();
            _rt.gc();
            Thread.yield();

            usedMem2 = usedMem1;
            usedMem1 = usedMemory();
        }
    }

    private static class MyEntry {
        final private static Random HASH_CREATOR = new Random();

        @Override
        public int hashCode() {
            return HASH_CREATOR.nextInt();
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }
}
