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

package org.openspaces.utest.persistency.cassandra.cache;

import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;

import junit.framework.Assert;

import org.junit.Test;
import org.openspaces.persistency.cassandra.NamedLockProvider;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class NamedLockProviderTest {
    private final Random random = new Random();

    private final int _numberOfConcurrentClients = 10;
    private final int _maxID = 500;

    private final NamedLockProvider _namedLock = new NamedLockProvider();
    private final Set<ReentrantLock> _locks = new ConcurrentHashSet<ReentrantLock>();
    private final Set<Integer> _lockIDs = new ConcurrentHashSet<Integer>();

    private volatile boolean _wait = true;
    private volatile boolean _run = true;

    @Test
    public void test() throws Exception {
        LockProviderClient[] clients = new LockProviderClient[_numberOfConcurrentClients];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new LockProviderClient();
            clients[i].start();
        }

        _wait = false;

        Thread.sleep(100);

        _run = false;

        for (int i = 0; i < clients.length; i++)
            clients[i].join();

        Assert.assertEquals("Unexpected number of locks created", _lockIDs.size(), _locks.size());
    }

    private class LockProviderClient extends Thread {
        @Override
        public void run() {
            while (_wait) ;

            while (_run) {
                int lockID = random.nextInt(_maxID);
                ReentrantLock lock = _namedLock.forName("" + lockID);
                _locks.add(lock);
                _lockIDs.add(lockID);
            }
        }
    }

}
