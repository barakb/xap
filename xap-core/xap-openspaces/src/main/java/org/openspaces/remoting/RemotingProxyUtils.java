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

package org.openspaces.remoting;

import com.gigaspaces.document.SpaceDocument;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.util.MethodInvoker;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;

/**
 * A set of common code shared between different remoting proxies.
 *
 * @author kimchy
 */
public abstract class RemotingProxyUtils {

    private RemotingProxyUtils() {

    }

    /**
     * Computes the routing index for a given remote invocation.
     */
    public static Object computeRouting(SpaceRemotingInvocation remotingEntry, RemoteRoutingHandler remoteRoutingHandler,
                                        MethodInvocation methodInvocation) throws Exception {
        Object routing = null;
        if (remoteRoutingHandler != null) {
            routing = remoteRoutingHandler.computeRouting(remotingEntry);
        }
        if (routing == null) {
            Annotation[][] parametersAnnotations = methodInvocation.getMethod().getParameterAnnotations();
            for (int i = 0; i < parametersAnnotations.length; i++) {
                Annotation[] parameterAnnotations = parametersAnnotations[i];
                for (Annotation parameterAnnotation : parameterAnnotations) {
                    if (parameterAnnotation instanceof Routing) {
                        Routing routingAnnotation = (Routing) parameterAnnotation;
                        if (StringUtils.hasLength(routingAnnotation.value())) {
                            Object parameter = methodInvocation.getArguments()[i];
                            if (parameter instanceof SpaceDocument) {
                                routing = ((SpaceDocument) parameter).getProperty(routingAnnotation.value());
                            } else {
                                MethodInvoker methodInvoker = new MethodInvoker();
                                methodInvoker.setTargetObject(parameter);
                                methodInvoker.setTargetMethod(routingAnnotation.value());
                                methodInvoker.prepare();
                                routing = methodInvoker.invoke();
                            }
                        } else {
                            routing = methodInvocation.getArguments()[i];
                        }
                        i = parametersAnnotations.length;
                        break;
                    }
                }
            }
        }
        if (routing == null) {
            routing = remotingEntry.hashCode();
        }
        return routing;
    }
}
