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

package net.jini.io.context;

/**
 * Defines a context element interface for determining if object integrity is being enforced on a
 * stream or a remote call.
 *
 * @author Sun Microsystems, Inc.
 * @see net.jini.core.constraint.Integrity
 * @see net.jini.export.ServerContext
 * @see net.jini.io.ObjectStreamContext
 * @since 2.0
 */
public interface IntegrityEnforcement {
    /**
     * Returns <code>true</code> if object integrity is being enforced, and <code>false</code>
     * otherwise.
     *
     * @return <code>true</code> if object integrity is being enforced, and <code>false</code>
     * otherwise
     */
    boolean integrityEnforced();
}
