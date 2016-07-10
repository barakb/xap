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

package org.openspaces.launcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;

/**
 * @author Niv Ingberg
 * @since 10.0.0
 */
public class JettyLauncher extends WebLauncher {

    private static final Log logger = LogFactory.getLog(JettyLauncher.class);

    @Override
    public void launch(WebLauncherConfig config) throws Exception {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(config.getPort());
        //GS-12102, fix for 10.1, added possibility to define host address
        if (config.getHostAddress() != null) {
            connector.setHost(config.getHostAddress());
        }
        connector.setReuseAddress(false);
        server.setConnectors(new Connector[]{connector});

        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(config.getWarFilePath());

        File tempDir = new File(config.getTempDirPath());
        boolean createdDirs = tempDir.mkdirs();

        if (logger.isDebugEnabled()) {
            boolean canRead = tempDir.canRead();
            boolean canWrite = tempDir.canWrite();
            boolean canExecute = tempDir.canExecute();

            logger.debug("Temp dir:" + tempDir.getName() + ", canRead=" + canRead + ", canWrite=" + canWrite +
                    ", canExecute=" + canExecute + ", exists=" + tempDir.exists() +
                    ", createdDirs=" + createdDirs + ", path=" + config.getTempDirPath());
        }

        webAppContext.setTempDirectory(tempDir);
        webAppContext.setCopyWebDir(false);
        webAppContext.setParentLoaderPriority(true);

        String sessionManager = System.getProperty("org.openspaces.launcher.jetty.session.manager");
        if (sessionManager != null) {
            //change default session manager implementation ( in order to change "JSESSIONID" )
            //GS-10830
            try {
                Class sessionManagerClass = Class.forName(sessionManager);
                SessionManager sessionManagerImpl = (SessionManager) sessionManagerClass.newInstance();
                webAppContext.getSessionHandler().setSessionManager(sessionManagerImpl);
            } catch (Throwable t) {
                System.out.println("Session Manager [" + sessionManager + "] was not set cause following exception:" + t.toString());
                t.printStackTrace();
            }
        } else {
            System.out.println("Session Manager was not provided");
        }

        server.setHandler(webAppContext);

        server.start();
        webAppContext.start();
    }
}
