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


package com.gigaspaces.security;

import java.io.Serializable;

/**
 * Interface for all authorities that may be granted to a user. "Authority" refers to an abstraction
 * given to a set of privileges (permissions). <p> An <tt>Authority</tt> must be representable as a
 * <tt>String</tt> which is specifically supported by an {@link AuthorityFactory}. <p>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public interface Authority extends Serializable {

    /**
     * An <code>Authority</code> that can be represented as a <code>String</code> which is
     * sufficient in precision to be relied upon for an access control decisions.
     *
     * @return a representation of the granted authority (expressed as a <code>String</code> with
     * sufficient precision).
     */
    String getAuthority();
}
