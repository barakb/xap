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

package com.gigaspaces.serialization.pbs;

/**
 * An object that is associated to a .NET AppDomain.
 *
 * @author eitany
 * @since 7.1
 */
public interface IDotnetProxyAssociated {
    /**
     * @return the id of the associated AppDomain, throws an exception if this object is not
     * associated to any AppDomain. {@link #hasAssociatedAppDomain()}
     */
    int getAppDomainId();

    /**
     * associate this object to the specified AppDomain id
     */
    void setAppDomainId(int appDomainId);

    /**
     * @return true if this object is associated to an AppDomain.
     */
    boolean hasAssociatedAppDomain();

    /**
     * Sets a dotnet proxy handle id
     *
     * @param handleId handle id of the proxy
     */
    void setDotnetProxyHandleId(long handleId);

    /**
     * @return true if this object is a target .NET proxy
     */
    boolean isTargetOfADotnetProxy();

    /**
     * @return the handle id of this object that the proxy is using.
     */
    long getDotnetProxyHandleId();
}
