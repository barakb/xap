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

package org.openspaces.pu.container.jee.jetty;

import com.j_spaces.core.IJSpace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ScopedHandler;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.LazyList;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.jee.sessions.jetty.GigaSessionIdManager;
import org.openspaces.jee.sessions.jetty.GigaSessionManager;
import org.openspaces.pu.container.jee.JeeProcessingUnitContainerProvider;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Field;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionListener;

/**
 * An jetty specific {@link javax.servlet.ServletContextListener} that is automatically loaded by
 * the {@link org.openspaces.pu.container.jee.context.BootstrapWebApplicationContextListener}. <p/>
 * <p>Support specific GigaSpace based session storge when using the
 * <code>jetty.sessions.spaceUrl</code> parameter within the (web) processing unit properties. It is
 * handled here since we want to setup the session support under the web application class loader
 * and not under the class loader that starts up jetty.
 *
 * @author kimchy
 */
public class JettyWebApplicationContextListener implements ServletContextListener {

    private static final Log logger = LogFactory.getLog(JettyWebApplicationContextListener.class);

    /**
     * A deploy property that controls if Jetty will store the session on the Space. Just by
     * specifying the url it will automatically enable it.
     */
    public static final String JETTY_SESSIONS_URL = "jetty.sessions.spaceUrl";

    /**
     * How often the scavenger thread will run in order to check for expired sessions. Set in
     * <b>seconds</b> and defaults to <code>60 * 5</code> seconds (5 minutes).
     */
    public static final String JETTY_SESSIONS_SCAVENGE_PERIOD = "jetty.sessions.scavengePeriod";

    /**
     * How often an actual update of a <b>non dirty</b> session will be performed to the Space. Set
     * in <b>seconds</b> and defaults to <code>60</code> seconds.
     */
    public static final String JETTY_SESSIONS_SAVE_PERIOD = "jetty.sessions.savePeriod";

    /**
     * The lease of the {@link org.openspaces.jee.sessions.jetty.SessionData} that is written to the
     * Space. Set in <b>seconds</b> and defaults to FOREVER.
     */
    public static final String JETTY_SESSIONS_LEASE = "jetty.sessions.lease";

    /**
     * Controls, using a deployment property, the timeout value of sessions. Set in <b>minutes</b>.
     */
    public static final String JETTY_SESSIONS_TIMEOUT = "jetty.sessions.timeout";

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        ServletContext servletContext = servletContextEvent.getServletContext();

        BeanLevelProperties beanLevelProperties = (BeanLevelProperties) servletContext.getAttribute(JeeProcessingUnitContainerProvider.BEAN_LEVEL_PROPERTIES_CONTEXT);
        ClusterInfo clusterInfo = (ClusterInfo) servletContext.getAttribute(JeeProcessingUnitContainerProvider.CLUSTER_INFO_CONTEXT);
        if (beanLevelProperties != null) {

            // automatically enable GigaSpaces Session Manager when passing the relevant property
            String sessionsSpaceUrl = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_URL);
            if (sessionsSpaceUrl != null) {
                logger.info("Jetty GigaSpaces Session support using space url [" + sessionsSpaceUrl + "]");
                // a hack to get the jetty context
                ServletContextHandler jettyContext = (ServletContextHandler) ((ContextHandler.Context) servletContext).getContextHandler();
                SessionHandler sessionHandler = jettyContext.getSessionHandler();

                try {
                    sessionHandler.stop();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to stop session handler to inject our own session manager", e);
                }

                GigaSessionManager gigaSessionManager = new GigaSessionManager();
                gigaSessionManager.setSpaceUrl(sessionsSpaceUrl);
                gigaSessionManager.setBeanLevelProperties(beanLevelProperties);
                gigaSessionManager.setClusterInfo(clusterInfo);

                if (sessionsSpaceUrl.startsWith("bean://")) {
                    ApplicationContext applicationContext = (ApplicationContext) servletContext.getAttribute(JeeProcessingUnitContainerProvider.APPLICATION_CONTEXT_CONTEXT);
                    if (applicationContext == null) {
                        throw new IllegalStateException("Failed to find servlet context bound application context");
                    }
                    IJSpace space;
                    Object bean = applicationContext.getBean(sessionsSpaceUrl.substring("bean://".length()));
                    if (bean instanceof GigaSpace) {
                        space = ((GigaSpace) bean).getSpace();
                    } else if (bean instanceof IJSpace) {
                        space = (IJSpace) bean;
                    } else {
                        throw new IllegalArgumentException("Bean [" + bean + "] is not of either GigaSpace type or IJSpace type");
                    }
                    gigaSessionManager.setSpace(space);
                }

                String scavangePeriod = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_SCAVENGE_PERIOD);
                if (scavangePeriod != null) {
                    gigaSessionManager.setScavengePeriod(Integer.parseInt(scavangePeriod));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Setting scavenge period to [" + scavangePeriod + "] seconds");
                    }
                }
                String savePeriod = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_SAVE_PERIOD);
                if (savePeriod != null) {
                    gigaSessionManager.setSavePeriod(Integer.parseInt(savePeriod));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Setting save period to [" + savePeriod + "] seconds");
                    }
                }
                String lease = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_LEASE);
                if (lease != null) {
                    gigaSessionManager.setLease(Long.parseLong(lease));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Setting lease to [" + lease + "] milliseconds");
                    }
                }

                // copy over session settings

                SessionManager sessionManager = sessionHandler.getSessionManager();
                gigaSessionManager.getSessionCookieConfig().setName(sessionManager.getSessionCookieConfig().getName());
                gigaSessionManager.getSessionCookieConfig().setDomain(sessionManager.getSessionCookieConfig().getDomain());
                gigaSessionManager.getSessionCookieConfig().setPath(sessionManager.getSessionCookieConfig().getPath());
                gigaSessionManager.setUsingCookies(sessionManager.isUsingCookies());
                gigaSessionManager.getSessionCookieConfig().setMaxAge(sessionManager.getSessionCookieConfig().getMaxAge());
                gigaSessionManager.getSessionCookieConfig().setSecure(sessionManager.getSessionCookieConfig().isSecure());
                gigaSessionManager.setMaxInactiveInterval(sessionManager.getMaxInactiveInterval());
                gigaSessionManager.setHttpOnly(sessionManager.getHttpOnly());
                gigaSessionManager.getSessionCookieConfig().setComment(sessionManager.getSessionCookieConfig().getComment());

                String sessionTimeout = beanLevelProperties.getContextProperties().getProperty(JETTY_SESSIONS_TIMEOUT);
                if (sessionTimeout != null) {
                    gigaSessionManager.setMaxInactiveInterval(Integer.parseInt(sessionTimeout) * 60);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Setting session timeout to [" + sessionTimeout + "] seconds");
                    }
                }

                GigaSessionIdManager sessionIdManager = new GigaSessionIdManager(jettyContext.getServer());
                sessionIdManager.setWorkerName(clusterInfo.getUniqueName().replace('.', '_'));
                gigaSessionManager.setIdManager(sessionIdManager);

                // copy over the session listeners
                try {
                    Field field = AbstractSessionManager.class.getDeclaredField("_sessionAttributeListeners");
                    field.setAccessible(true);
                    Object sessionAttributeListeners = field.get(sessionHandler.getSessionManager());
                    if (sessionAttributeListeners != null) {
                        for (int i = 0; i < LazyList.size(sessionAttributeListeners); i++) {
                            gigaSessionManager.addEventListener((HttpSessionAttributeListener) LazyList.get(sessionAttributeListeners, i));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to copy over _sessionAttributeListeners", e);
                }
                try {
                    Field field = AbstractSessionManager.class.getDeclaredField("_sessionListeners");
                    field.setAccessible(true);
                    Object sessionListeners = field.get(sessionHandler.getSessionManager());
                    if (sessionListeners != null) {
                        for (int i = 0; i < LazyList.size(sessionListeners); i++) {
                            gigaSessionManager.addEventListener((HttpSessionListener) LazyList.get(sessionListeners, i));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to copy over _sessionListeners", e);
                }

                sessionHandler.setSessionManager(gigaSessionManager);

                try {
                    sessionHandler.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start session handler to inject our own session manager", e);
                }

                // HACK BARK CRACK
                // It seems like when stopping and then starting a session handler, the outerScope of the ServletHandler (the
                // next scope after the web context) gets nulled, meaning that the handle won't be called, resulting in
                // the session handler not being called at all.
                // For now, set it explicitly using reflection until I figure out with jetty guys what can be done to
                // fix this.
                try {
                    ServletHandler servletHandler = (ServletHandler) sessionHandler.getChildHandlerByClass(ServletHandler.class);
                    Field outerScopeField = ScopedHandler.class.getDeclaredField("_outerScope");
                    outerScopeField.setAccessible(true);
                    outerScopeField.set(servletHandler, jettyContext);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to set outer scope on ServletHandler (workaround jetty bug)", e);
                }

            }

            // if we have a simple hash session id manager, set its worker name automatically...
            ServletContextHandler jettyContext = (ServletContextHandler) ((ContextHandler.Context) servletContext).getContextHandler();
            SessionHandler sessionHandler = jettyContext.getSessionHandler();
            // automatically set the worker name
            if (sessionHandler.getSessionManager().getSessionIdManager() instanceof HashSessionIdManager) {
                HashSessionIdManager sessionIdManager = (HashSessionIdManager) sessionHandler.getSessionManager().getSessionIdManager();
                if (sessionIdManager.getWorkerName() == null) {
                    sessionIdManager.setWorkerName(clusterInfo.getUniqueName().replace('.', '_'));
                    if (logger.isDebugEnabled()) {
                        logger.debug("Automatically setting worker name to [" + sessionIdManager.getWorkerName() + "]");
                    }
                }
            }
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
}
