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

package com.gigaspaces.internal.utils;

import com.j_spaces.core.service.ServiceConfigLoader;

import net.jini.config.ConfigurationException;
import net.jini.lease.LeaseRenewalManager;
import net.jini.space.InternalSpaceException;

@com.gigaspaces.api.InternalApi
public class SharedObjects {
    public static final SharedLeaseRenewalManager LeaseRenewalManager = new SharedLeaseRenewalManager();

    public static abstract class SharedObject<T> {
        private T _instance;
        private int _references;

        public synchronized T get() {
            if (_instance == null) {
                _instance = createInstance();
                _references = 0;
            }
            _references++;
            return _instance;
        }

        public synchronized void release() {
            if (--_references <= 0 && _instance != null) {
                closeInstance(_instance);
                _instance = null;
            }
        }

        protected abstract T createInstance();

        protected abstract void closeInstance(T instance);
    }

    public static class SharedLeaseRenewalManager extends SharedObject<LeaseRenewalManager> {
        @Override
        protected LeaseRenewalManager createInstance() {
            try {
                return new LeaseRenewalManager(ServiceConfigLoader.getConfiguration());
            } catch (ConfigurationException e) {
                throw new InternalSpaceException("Failed to instantiate shared LeaseRenewalManager", e);
            }
        }

        @Override
        protected void closeInstance(LeaseRenewalManager instance) {
            instance.terminate();
        }
    }
}
