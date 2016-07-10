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

package com.j_spaces.kernel;

import com.gigaspaces.start.Locator;
import com.j_spaces.core.Constants;

import java.net.URL;
import java.rmi.RMISecurityManager;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;

/**
 * Utility class to load a security policy for a class and set the policy as the JVM's security
 * policy. If java.security.policy property is not set and no SecurityManager been set, it loads a
 * default /policy/gigaspaces.policy security file.
 *
 * @author Gershon Diner
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class SecurityPolicyLoader {

    /**
     * Load and set the java.security.policy property for a Class.
     *
     * @param policyName - The name of the policy file to load
     */
    public static void loadPolicy(String policyName) {
        Policy.setPolicy(new Policy() {
            public PermissionCollection getPermissions(CodeSource codesource) {
                Permissions perms = new Permissions();
                perms.add(new AllPermission());
                return (perms);
            }

            public void refresh() {
            }

        });
        URL policy = null;
        String securityFilePath = null;
        try {
            //don't use here the ResourceLoader since it needs the logger first to be initialzed.
            policy = Thread.currentThread().getContextClassLoader().getResource(policyName);
            if (policy == null) {
                securityFilePath = Locator.derivePath("policy.all");
                if (securityFilePath == null)
                    System.err.println("WARNING: can't find [" + Constants.System.SYSTEM_GS_POLICY + "] resource");
                    // not a fatal error, but a SecurityException is probable.
                else
                    System.setProperty("java.security.policy", securityFilePath);
            } else {
                System.setProperty("java.security.policy", policy.toString());
            }
            if (System.getSecurityManager() == null) {
                System.setSecurityManager(new RMISecurityManager());
            }
            System.getSecurityManager().checkPermission(new RuntimePermission("getClassLoader"));
        } catch (SecurityException uh_oh) {
            String policyStr = policyName;
            if (policy != null)
                policyStr = policy.toString();
            System.err.println("SEVERE: Failed to set Java Security Manager using the security policy file at < " +
                    policyStr + " >. Please verify the java.security.policy system property is set.");
            uh_oh.printStackTrace();
        }
    }
}