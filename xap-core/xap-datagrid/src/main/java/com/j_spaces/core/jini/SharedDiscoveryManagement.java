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

package com.j_spaces.core.jini;

import com.gigaspaces.start.SystemBoot;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.ClassLoaderHelper;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lease.Lease;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.jini.rio.boot.CommonClassLoader;

import java.io.IOException;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper allowing to share and return cached LDM and SDM.
 *
 * <p>In order to get an LDM, the {@link #getLookupDiscoveryManager(String[],
 * net.jini.core.discovery.LookupLocator[], net.jini.discovery.DiscoveryListener)} should be called
 * and be used as normal. Terminate will reomve it from the cache if needed.
 *
 * <p>In order to get an SDM, the {@link #getBackwardsServiceDiscoveryManager(String[],
 * net.jini.core.discovery.LookupLocator[], net.jini.discovery.DiscoveryListener)} should be used
 * (internally, it will also use a shared LDM). NOTE: If a lookup cache is created using the SDM,
 * make sure to close it explicitly since terminating the SDM will not necesseraly actually
 * terminate it, leaving the lookup cache open.
 *
 * @author kimchy
 */
public final class SharedDiscoveryManagement {

    private final static Map<SharedDiscoEntry, SharedDiscoveryManager> ldms = new HashMap<SharedDiscoEntry, SharedDiscoveryManager>();
    private final static Map<SharedDiscoEntry, SharedServiceDiscoveryManager> sdms = new HashMap<SharedDiscoEntry, SharedServiceDiscoveryManager>();

    private static final Object mutex = new Object();

    private SharedDiscoveryManagement() {

    }

    public static LookupDiscoveryManager getLookupDiscoveryManager(
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener) throws IOException {
        synchronized (mutex) {
            SharedDiscoEntry entry = new SharedDiscoEntry(groups, locators, false, true);
            SharedDiscoveryManager ldm = ldms.get(entry);
            if (ldm == null) {
                // if we run under the GSC, open the SDM and LDM under the common class loader
                // so threads they open will not refernce the Service class loader
                ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
                if (SystemBoot.isRunningWithinGSC()) {
                    ClassLoaderHelper.setContextClassLoader(CommonClassLoader.getInstance(), true);
                }
                try {
                    Configuration configuration = ServiceConfigLoader.getConfiguration();
                    ldm = new SharedDiscoveryManager(groups, locators, null, configuration);
                    ldms.put(entry, ldm);
                } catch (ConfigurationException e) {
                    IOException ex = new IOException("Failed to configure ldm");
                    ex.initCause(e);
                    throw ex;
                } finally {
                    if (SystemBoot.isRunningWithinGSC()) {
                        ClassLoaderHelper.setContextClassLoader(origClassLoader, true);
                    }
                }
            }
            if (listener != null) {
                ldm.addDiscoveryListener(listener);
            }
            ldm.incrementRefCounter();
            return ldm;
        }
    }

    public static SharedServiceDiscoveryManager getBackwardsServiceDiscoveryManager(
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener) throws IOException {
        return getServiceDiscoveryManager(null /* ldm */,
                groups,
                locators,
                listener,
                true /* isBackwardCompatible */);
    }

    public static SharedServiceDiscoveryManager getServiceDiscoveryManager(
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener) throws IOException {
        return getServiceDiscoveryManager(null /* ldm */,
                groups,
                locators,
                listener,
                false /* isBackwardCompatible */);
    }

    public static SharedServiceDiscoveryManagerResult getServiceDiscoveryManagerResult(
            LookupDiscoveryManager ldm,
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener)
            throws IOException {
        return getServiceDiscoveryManagerResult(ldm,
                groups,
                locators,
                listener,
                false /* isBackwardsCompatible */);
    }

    private static SharedServiceDiscoveryManager getServiceDiscoveryManager(
            LookupDiscoveryManager ldm,
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener,
            boolean isBackwardsCompatible) throws IOException {
        SharedServiceDiscoveryManagerResult result =
                getServiceDiscoveryManagerResult(ldm,
                        groups,
                        locators,
                        listener,
                        isBackwardsCompatible);
        return result.sdm;
    }

    private static SharedServiceDiscoveryManagerResult getServiceDiscoveryManagerResult(
            LookupDiscoveryManager ldm,
            String[] groups,
            LookupLocator[] locators,
            DiscoveryListener listener,
            boolean isBackwardsCompatible) throws IOException {
        synchronized (mutex) {
            SharedServiceDiscoveryManager sdm;
            boolean isManaged;

            SharedServiceDiscoveryManager existingSdm = null;
            boolean sdmExists = false;
            if (ldm != null) {
                // checking both backward compatible and new
                SharedDiscoEntry existingSdmSearchEntry = new SharedDiscoEntry(groups, locators, false, true);
                existingSdm = sdms.get(existingSdmSearchEntry);
                if (existingSdm == null || existingSdm.getDiscoveryManager() != ldm) {
                    existingSdmSearchEntry = new SharedDiscoEntry(groups, locators, true, true);
                    existingSdm = sdms.get(existingSdmSearchEntry);
                }

                sdmExists = existingSdm != null && existingSdm.getDiscoveryManager() == ldm;
            }

            if (sdmExists) {
                sdm = existingSdm;
                isManaged = true;
            } else {
                SharedDiscoEntry entry = new SharedDiscoEntry(groups, locators, isBackwardsCompatible, ldm == null);
                sdm = sdms.get(entry);
                if (sdm == null) {
                    // if we run under the GSC, open the SDM and LDM under the common class loader
                    // so threads they open will not refernce the Service class loader
                    ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
                    if (SystemBoot.isRunningWithinGSC()) {
                        ClassLoaderHelper.setContextClassLoader(CommonClassLoader.getInstance(), true);
                    }
                    try {
                        if (entry.isManagingLookupDiscoveryManager()) {
                            ldm = getLookupDiscoveryManager(groups, locators, listener);
                        }

                        if (entry.isBackwardsCompatible()) {
                            sdm = new BackwardsSharedServiceDiscoveryManager(entry, ldm, SharedLeaseRenewalManager.getLeaseRenewalManager(), ServiceConfigLoader.getConfiguration());
                        } else {
                            sdm = new SharedServiceDiscoveryManager(entry, ldm, SharedLeaseRenewalManager.getLeaseRenewalManager(), ServiceConfigLoader.getConfiguration());
                        }
                        sdms.put(entry, sdm);
                    } catch (ConfigurationException e) {
                        IOException ex = new IOException("Failed to configure ldm");
                        ex.initCause(e);
                        throw ex;
                    } finally {
                        if (SystemBoot.isRunningWithinGSC()) {
                            ClassLoaderHelper.setContextClassLoader(origClassLoader, true);
                        }
                    }
                } else if (entry.isManagingLookupDiscoveryManager()) {
                    //calling getLookupDiscoveryManager will add this listener to the LDM and increment the ref. count 
                    ldm = getLookupDiscoveryManager(groups, locators, listener);
                    if (ldm != sdm.getDiscoveryManager()) {
                        //make sure we are adding the listener to the correct LDM - otherwise this is not the LDM used by the SDM.
                        throw new ConcurrentModificationException("lookup discovery manager was modified");
                    }
                }
                isManaged = false;
            }

            sdm.incrementRefCounter();
            return new SharedServiceDiscoveryManagerResult(sdm, isManaged);
        }
    }

    public static void forceTerminate() {
        synchronized (mutex) {
            for (SharedDiscoveryManager ldm : ldms.values()) {
                ldm.forceTerminate();
            }
            ldms.clear();
            for (SharedServiceDiscoveryManager sdm : sdms.values()) {
                sdm.forceTerminate();
            }
            sdms.clear();
        }
    }

    @Deprecated
    public static class BackwardsSharedServiceDiscoveryManager extends SharedServiceDiscoveryManager {

        public BackwardsSharedServiceDiscoveryManager(SharedDiscoEntry entry,
                                                      LookupDiscoveryManager discoveryMgr,
                                                      LeaseRenewalManager leaseMgr, Configuration config)
                throws IOException, ConfigurationException {
            super(entry, discoveryMgr, leaseMgr, config);
        }

        private static final long BACKWARDS_COMPATIBLE_NOTIFICATIONS_LEASE_RENEW_RATE = Lease.FOREVER;
        private static final long BACKWARDS_COMPATIBLE_REMOVE_SERVICE_IF_ORPHAN_DELAY = 0;

        @Override
        protected long getDefaultNotificationsLeaseRenewalRate() {
            return BACKWARDS_COMPATIBLE_NOTIFICATIONS_LEASE_RENEW_RATE;
        }

        @Override
        protected long getDefaultRemoveServiceIfOrphanDelay() {
            return BACKWARDS_COMPATIBLE_REMOVE_SERVICE_IF_ORPHAN_DELAY;
        }

    }

    public static class SharedServiceDiscoveryManager extends ServiceDiscoveryManager {

        private int refCounter = 0;

        private final SharedDiscoEntry entry;

        private final LookupDiscoveryManager ldm;

        public SharedServiceDiscoveryManager(SharedDiscoEntry entry, LookupDiscoveryManager discoveryMgr, LeaseRenewalManager leaseMgr, Configuration config) throws IOException, ConfigurationException {
            super(discoveryMgr, leaseMgr, config);
            this.entry = entry;
            this.ldm = discoveryMgr;
        }

        /**
         * Increment the references
         */
        void incrementRefCounter() {
            synchronized (mutex) {
                refCounter++;
            }
        }

        public int getRefCounter() {
            synchronized (mutex) {
                return refCounter;
            }
        }

        void forceTerminate() {
            synchronized (mutex) {
                super.terminate();
                if (entry.isManagingLookupDiscoveryManager()) {
                    // we terminate the LDM as well since we get a handle to a new ref counted one in the getSDM
                    ldm.terminate();
                }
                SharedLeaseRenewalManager.releaseLeaseRenewalManaer();
            }
        }

        @Override
        public void terminate() {
            synchronized (mutex) {
                refCounter--;
                if (refCounter == 0) {
                    super.terminate();
                    if (entry.isManagingLookupDiscoveryManager()) {
                        // we terminate the LDM as well since we get a handle to a new ref counted in the getSDM
                        ldm.terminate();
                    }
                    SharedLeaseRenewalManager.releaseLeaseRenewalManaer();

                    sdms.remove(entry);
                } else if (entry.isManagingLookupDiscoveryManager()) {
                    // we terminate the LDM as well since we get a handle to a new ref counted in the getSDM
                    ldm.terminate();
                }
            }
        }
    }

    public static class SharedDiscoveryManager extends LookupDiscoveryManager {

        private int refCounter = 0;

        private final SharedDiscoEntry entry;

        public SharedDiscoveryManager(String[] groups,
                                      LookupLocator[] locators,
                                      DiscoveryListener listener,
                                      Configuration config)
                throws IOException, ConfigurationException {
            super(groups,
                    locators,
                    listener,
                    config);
            this.entry = new SharedDiscoEntry(groups, locators, false, true);
        }

        /**
         * Increment the references
         */
        void incrementRefCounter() {
            synchronized (mutex) {
                refCounter++;
            }
        }

        void forceTerminate() {
            synchronized (mutex) {
                super.terminate();
            }
        }

        /**
         * Override parent's terminate method. Only call LookupDiscoveryManager.terminate() if there
         * are no clients or users of the
         */
        @Override
        public void terminate() {
            synchronized (mutex) {
                refCounter--;
                if (refCounter == 0) {
                    super.terminate();
                    ldms.remove(entry);
                }
            }
        }

    }

    public static class SharedServiceDiscoveryManagerResult {
        public final SharedServiceDiscoveryManager sdm;
        public final boolean isManaged;

        public SharedServiceDiscoveryManagerResult(SharedServiceDiscoveryManager sdm, boolean isManaged) {
            this.sdm = sdm;
            this.isManaged = isManaged;
        }
    }

    public static class SharedDiscoEntry {

        private final String[] groups;
        private final LookupLocator[] locators;
        private final boolean isBackwardsCompatible; //true behaves prior to GS-9875
        private final boolean isManagingLookupDiscoveryManager;

        public SharedDiscoEntry(String[] groups, LookupLocator[] locators, boolean isBackwardsCompatible,
                                boolean isManagingLookupDiscoveryManager) {
            this.groups = groups;
            this.locators = locators;
            this.isBackwardsCompatible = isBackwardsCompatible;
            this.isManagingLookupDiscoveryManager = isManagingLookupDiscoveryManager;
        }

        public boolean isBackwardsCompatible() {
            return isBackwardsCompatible;
        }

        public boolean isManagingLookupDiscoveryManager() {
            return isManagingLookupDiscoveryManager;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(groups);
            result = prime * result + (isBackwardsCompatible ? 1231 : 1237);
            result = prime * result + (isManagingLookupDiscoveryManager ? 1231 : 1237);
            result = prime * result + Arrays.hashCode(locators);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SharedDiscoEntry other = (SharedDiscoEntry) obj;
            if (!Arrays.equals(groups, other.groups))
                return false;
            if (isBackwardsCompatible != other.isBackwardsCompatible)
                return false;
            if (isManagingLookupDiscoveryManager != other.isManagingLookupDiscoveryManager)
                return false;
            if (!Arrays.equals(locators, other.locators))
                return false;
            return true;
        }

    }

}
