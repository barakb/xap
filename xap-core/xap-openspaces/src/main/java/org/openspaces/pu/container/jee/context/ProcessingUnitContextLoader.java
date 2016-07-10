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


package org.openspaces.pu.container.jee.context;

import com.gigaspaces.internal.dump.InternalDumpProcessor;

import org.jini.rio.boot.SharedServiceData;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.MemberAliveIndicator;
import org.openspaces.core.cluster.ProcessingUnitUndeployingListener;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.pu.container.ProcessingUnitContainerConfig;
import org.openspaces.pu.container.jee.JeeProcessingUnitContainerProvider;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.ServletContext;

/**
 * Same as Spring {@link org.springframework.web.context.ContextLoader}. Different in two aspects:
 * <p/> <p>The first, it automatillcay loads the binded <code>META-INF/spring/pu.xml</code> (binded
 * into the servlet context) as the parent application context. See {@link
 * #loadParentContext(javax.servlet.ServletContext)}. <p/> <p>Second, it overrides the creation of
 * {@link org.springframework.web.context.WebApplicationContext} and automatically adds {@link
 * org.openspaces.core.cluster.ClusterInfo} and {@link org.openspaces.core.properties.BeanLevelProperties}
 * handling. It also delegates the objects to the web context level Spring application context.
 *
 * @author kimchy
 */
public class ProcessingUnitContextLoader extends ContextLoader {

    @Override
    public WebApplicationContext initWebApplicationContext(ServletContext servletContext) throws IllegalStateException, BeansException {
        final WebApplicationContext context = super.initWebApplicationContext(servletContext);
        ClusterInfo clusterInfo = (ClusterInfo) servletContext.getAttribute(JeeProcessingUnitContainerProvider.CLUSTER_INFO_CONTEXT);
        if (clusterInfo != null) {
            String key = clusterInfo.getUniqueName();

            SharedServiceData.addServiceDetails(key, new Callable() {
                public Object call() throws Exception {
                    ArrayList<Object> serviceDetails = new ArrayList<Object>();
                    Map map = context.getBeansOfType(ServiceDetailsProvider.class);
                    for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                        ServiceDetails[] details = ((ServiceDetailsProvider) it.next()).getServicesDetails();
                        if (details != null) {
                            for (ServiceDetails detail : details) {
                                serviceDetails.add(detail);
                            }
                        }
                    }
                    return serviceDetails.toArray(new Object[serviceDetails.size()]);
                }
            });

            SharedServiceData.addServiceMonitors(key, new Callable() {
                public Object call() throws Exception {
                    ArrayList<ServiceMonitors> serviceMonitors = new ArrayList<ServiceMonitors>();
                    Map map = context.getBeansOfType(ServiceMonitorsProvider.class);
                    for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                        ServiceMonitors[] monitors = ((ServiceMonitorsProvider) it.next()).getServicesMonitors();
                        if (monitors != null) {
                            for (ServiceMonitors monitor : monitors) {
                                serviceMonitors.add(monitor);
                            }
                        }
                    }
                    return serviceMonitors.toArray(new Object[serviceMonitors.size()]);
                }
            });

            Map map = context.getBeansOfType(MemberAliveIndicator.class);
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                final MemberAliveIndicator memberAliveIndicator = (MemberAliveIndicator) it.next();
                if (memberAliveIndicator.isMemberAliveEnabled()) {
                    SharedServiceData.addMemberAliveIndicator(key, new Callable<Boolean>() {
                        public Boolean call() throws Exception {
                            return memberAliveIndicator.isAlive();
                        }
                    });
                }
            }

            map = context.getBeansOfType(ProcessingUnitUndeployingListener.class);
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                final ProcessingUnitUndeployingListener listener = (ProcessingUnitUndeployingListener) it.next();
                SharedServiceData.addUndeployingEventListener(key, new Callable() {
                    public Object call() throws Exception {
                        listener.processingUnitUndeploying();
                        return null;
                    }
                });
            }

            map = context.getBeansOfType(InternalDumpProcessor.class);
            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                SharedServiceData.addDumpProcessors(key, it.next());
            }
        }
        return context;
    }

    /**
     * Returns the application context bound under {@link org.openspaces.pu.container.jee.JeeProcessingUnitContainerProvider#APPLICATION_CONTEXT_CONTEXT}
     * within the servlet context. This will act as the parent application context.
     */
    protected ApplicationContext loadParentContext(ServletContext servletContext) throws BeansException {
        return (ApplicationContext) servletContext.getAttribute(JeeProcessingUnitContainerProvider.APPLICATION_CONTEXT_CONTEXT);
    }

    /**
     * Creates a Spring {@link org.springframework.web.context.WebApplicationContext} - {@link
     * org.openspaces.pu.container.jee.context.ProcessingUnitWebApplicationContext}. <p/> <p>Adds
     * support to {@link ClusterInfo} and {@link org.openspaces.core.properties.BeanLevelProperties}
     * processors. The objects themself are bounded in the {@link javax.servlet.ServletContext}.
     */
    protected WebApplicationContext createWebApplicationContext(ServletContext servletContext, ApplicationContext parent) throws BeansException {
        ProcessingUnitContainerConfig config = new ProcessingUnitContainerConfig();
        config.setClusterInfo((ClusterInfo) servletContext.getAttribute(JeeProcessingUnitContainerProvider.CLUSTER_INFO_CONTEXT));
        config.setBeanLevelProperties((BeanLevelProperties) servletContext.getAttribute(JeeProcessingUnitContainerProvider.BEAN_LEVEL_PROPERTIES_CONTEXT));
        ProcessingUnitWebApplicationContext wac = new ProcessingUnitWebApplicationContext(config);
        wac.setParent(parent);
        wac.setServletContext(servletContext);
        wac.setConfigLocation(servletContext.getInitParameter(CONFIG_LOCATION_PARAM));
        customizeContext(servletContext, wac);
        wac.refresh();
        return wac;

    }
}
