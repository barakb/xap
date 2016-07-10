/*
 * Copyright GigaSpaces Technologies Inc. 2006
 */

package org.jini.rio.jmx;

import java.lang.reflect.Method;

import javax.management.MBeanServer;

/**
 * Created by IntelliJ IDEA. User: ming Date: Nov 25, 2006 Time: 2:38:29 PM
 */
@com.gigaspaces.api.InternalApi
public class MBeanServerFactory {
    public static MBeanServer server;

    /**
     * Get the MBeanServer to use. If the Java Virutal Machine is java 1.5, the platform MBeanServer
     * is returned, otherwise the MBeanServer will be created using {@link
     * javax.management.MBeanServerFactory#createMBeanServer()}
     *
     * @return the <code>MBeanServer</code>
     */
    public static synchronized MBeanServer getMBeanServer() {
        if (server == null) {
            try {
                //try 1.5 first
                Class mfClass =
                        Class.forName("java.lang.management.ManagementFactory");
                Method m = mfClass.getMethod(
                        "getPlatformMBeanServer",
                        (Class[]) null
                );
                Object mbs = m.invoke(null, (Object[]) null);
                server = (MBeanServer) mbs;
            } catch (Exception e) {
                //1.4
                server = javax.management.MBeanServerFactory.createMBeanServer();
            }
        }
        return server;
    }
}
