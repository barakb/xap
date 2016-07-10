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

/*
 * @(#)RemoteMethodCache.java 1.0  07/05/2005 21:09:17
 */

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.reflection.IMethod;

import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map.Entry;

/**
 * This class contains a sorted method-map of exported Remote Object with fast method search by
 * IdentityHashMap.
 *
 * @author Igor Goldenberg
 * @see com.gigaspaces.lrmi.LRMIMethod
 * @see com.j_spaces.kernel.lrmi.LRMIUtilities.getRemoteMethodCache()
 * @since 4.0
 **/
@com.gigaspaces.api.InternalApi
public class RemoteMethodCache {
    final private HashMap<IMethod, LRMIMethod> methodMap;
    private IdentityHashMap<IMethod, LRMIMethod> identityMethodMap = new IdentityHashMap<IMethod, LRMIMethod>();

    /**
     * Constructor.
     *
     * @param lrmiMethodList Sorted Remote Method's list of exported Remote object.
     **/
    public RemoteMethodCache(LRMIMethod[] lrmiMethodList) {
        /** keep mapping between really method and orderId */
        methodMap = new HashMap<IMethod, LRMIMethod>(lrmiMethodList.length);
        for (int i = 0; i < lrmiMethodList.length; i++)
            methodMap.put(lrmiMethodList[i].realMethod, lrmiMethodList[i]);
    }

    /**
     * Returns the ~cached LRMI method structure.
     *
     * @param method The reflection method.
     **/
    public LRMIMethod getLRMIMethod(IMethod method) {
       /* get LRMIMethod by Identity method hashCode() */
        LRMIMethod lrmiMethod = identityMethodMap.get(method);

        if (lrmiMethod != null)
            return lrmiMethod;

        synchronized (this) {
            lrmiMethod = identityMethodMap.get(method);
            if (lrmiMethod != null)
                return lrmiMethod;

    	   /* get the already cached invoked method */
            lrmiMethod = methodMap.get(method);

    	   /* 
            * if null handle a special case where the invoked method not from the same ClassLoader of
    	    * cached method.
    	    * find the cached method not by equals() --> by method description (String.equals) 
    	    * Cross ClassLoading equals() solution.
    	    */
            if (lrmiMethod == null) {
                String invokedMethod = LRMIUtilities.getMethodNameAndDescriptor(method);

                for (Entry<IMethod, LRMIMethod> cacheMethod : methodMap.entrySet()) {
                    String cachedMethodDesc = LRMIUtilities.getMethodNameAndDescriptor(cacheMethod.getKey());
                    if (cachedMethodDesc.equals(invokedMethod)) {
                        lrmiMethod = cacheMethod.getValue();
                        break;
                    }
                }// for

    		   /* unexpected behavior where the invoked method was not found in the method mapping table */
                if (lrmiMethod == null) {
                    UnexpectedException ex = new UnexpectedException("Failed to invoke remote method: [" +
                            method + "]. This method was never introduced by transport protocol. " +
                            "\nMethod ClassLoader: " + method.getDeclaringClass().getClassLoader());
                    throw new RuntimeException(ex);
                }
            }// if

    	   /* save synchronize lock. Before put into ~cache table, clone() the table because of thread race condition */
            IdentityHashMap<IMethod, LRMIMethod> cloneMap = (IdentityHashMap) identityMethodMap.clone();
            cloneMap.put(method, lrmiMethod);
            identityMethodMap = cloneMap;
        }

        return lrmiMethod;
    }
}