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

package net.jini.url.https;

import net.jini.security.IntegrityVerifier;

import java.net.URL;

/**
 * Integrity verifier for HTTPS URLs. This class is intended to be specified in a resource to
 * configure the operation of {@link net.jini.security.Security#verifyCodebaseIntegrity
 * Security.verifyCodebaseIntegrity}. <p> This verifier assumes the HTTPS URL protocol handler is
 * configured to provide adequate data integrity and server authentication.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class HttpsIntegrityVerifier implements IntegrityVerifier {
    /**
     * Returns <code>true</code> if the specified URL uses the "https" protocol; returns
     * <code>false</code> otherwise.
     *
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean providesIntegrity(URL url) {
        return "https".equals(url.getProtocol());
    }
}
