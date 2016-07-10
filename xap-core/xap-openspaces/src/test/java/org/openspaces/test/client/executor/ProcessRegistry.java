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

package org.openspaces.test.client.executor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A lookup registry for processes.
 *
 * @author moran
 */
public class ProcessRegistry {

    private final int registryPort;
    private final Registry registry;

    private ProcessRegistry() {
        try {
            registryPort = ExecutorUtils.getAnonymousPort();
            registry = LocateRegistry.createRegistry(registryPort);
        } catch (RemoteException re) {
            throw new RegistryException("Failed to load process registry", re);
        }
    }

    private ProcessRegistry(int port, Registry registry) {
        registryPort = port;
        this.registry = registry;
    }

    /**
     * Creates and exports a Registry instance on the local host that accepts requests on an
     * anonymous port.
     *
     * @return a registry encapsulation.
     */
    public static ProcessRegistry createRegistry() {
        return new ProcessRegistry();
    }

    /**
     * Returns a reference to the the remote object Registry for the local host on the specified
     * port.
     *
     * @param port port on which the registry accepts requests.
     * @return a registry encapsulation.
     * @throws RegistryException if failed to locate registry.
     */
    public static ProcessRegistry locateRegistry(int port) throws RegistryException {
        try {
            Registry registry = LocateRegistry.getRegistry(port);
            ProcessRegistry processRegistry = new ProcessRegistry(port, registry);
            return processRegistry;
        } catch (RemoteException e) {
            throw new RegistryException("Failed to locate registry on port [" + port + "]", e);
        }
    }

    /**
     * @return port on which the registry accepts requests.
     */
    public int getRegistryPort() {
        return registryPort;
    }

    /**
     * Replaces the binding for the specified key in this registry with the supplied remote
     * reference. If there is an existing binding for the specified name, it is discarded.
     *
     * @param key   binding name
     * @param value binding reference
     * @throws RegistryException if failed to register
     */
    public void register(String key, Remote value) throws RegistryException {
        try {
            registry.rebind(key, value);
        } catch (Exception e) {
            throw new RegistryException("Failed to register with key [" + key + "]", e);
        }
    }

    /**
     * Removes the binding for the specified key in this registry.
     *
     * @param key binding name
     * @throws RegistryException if failed to unregister
     */
    public void unregister(String key) throws RegistryException {
        try {
            registry.unbind(key);
        } catch (Exception e) {
            throw new RegistryException("Failed to unregister with key [" + key + "]", e);
        }
    }

    /**
     * Returns the remote reference bound to the specified key in this registry.
     *
     * @param key the name for the remote reference to look up
     * @return a reference to a remote object
     */
    public Remote lookup(String key) throws RegistryException {
        try {
            return registry.lookup(key);
        } catch (Exception e) {
            throw new RegistryException("Failed to lookup with key [" + key + "]", e);
        }
    }

    /**
     * activates a watchdog watching over a registration. If lookup fails, will issue a process
     * termination.
     *
     * @param key the name for the remote reference to look up.
     */
    public void monitor(String key) {
        Timer t = new Timer(true /*isDaemon*/);
        t.schedule(new WatchableRegistration(key), 10 * 1000/*delay*/, 10 * 1000 /*interval*/);
    }

    @Override
    public String toString() {
        return "registry: [" + registry + "] port: [" + registryPort + "]";
    }

    /**
     * A watchdog task watching over a registration. If lookup fails, will issue a process
     * termination.
     */
    private final class WatchableRegistration extends TimerTask {

        private volatile boolean keepWatching = true;
        private final String key;

        public WatchableRegistration(String key) {
            this.key = key;
        }

        @Override
        public void run() {
            if (keepWatching) {
                try {
                    registry.lookup(key);
                } catch (Exception e) {
                    //die on next awakening
                    keepWatching = false;
                    ProcessLogger.log(" WatchableRegistration caught: " + e + "\n\t - forcibly terminating VM on next awakening");
                }
            } else {
                ProcessLogger.log(" WatchableRegistration forcibly terminating the currently running VM.");
                Runtime.getRuntime().halt(-1);
            }
        }
    }
}
