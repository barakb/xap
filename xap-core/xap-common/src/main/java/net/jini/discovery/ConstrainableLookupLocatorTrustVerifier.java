/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package net.jini.discovery;

import net.jini.security.TrustVerifier;

import java.rmi.RemoteException;

/**
 * Trust verifier for {@link ConstrainableLookupLocator} instances.  This class is intended to be
 * specified in a resource to configure the operation of {@link net.jini.security.Security#verifyObjectTrust
 * Security.verifyObjectTrust}.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class ConstrainableLookupLocatorTrustVerifier implements TrustVerifier {

    /**
     * Creates an instance.
     */
    public ConstrainableLookupLocatorTrustVerifier() {
    }

    /**
     * Returns <code>true</code> if the specified object is a trusted <code>ConstrainableLookupLocator</code>
     * instance; returns <code>false</code> otherwise.  For the purposes of this verifier, all
     * instances of <code>ConstrainableLookupLocator</code> are considered trusted.
     *
     * @throws SecurityException    {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean isTrustedObject(Object obj, TrustVerifier.Context ctx)
            throws RemoteException {
        return obj instanceof ConstrainableLookupLocator;
    }
}
