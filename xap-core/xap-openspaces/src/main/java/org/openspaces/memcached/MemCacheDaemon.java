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

package org.openspaces.memcached;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.memcached.protocol.UnifiedProtocolDecoder;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryPipelineFactory;
import org.openspaces.memcached.protocol.text.MemcachedPipelineFactory;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * @author kimchy (shay.banon)
 */
public class MemCacheDaemon implements InitializingBean, DisposableBean, BeanNameAware, ServiceDetailsProvider, ServiceMonitorsProvider {

    protected final Log logger = LogFactory.getLog(getClass());

    public final static String memcachedVersion = "0.9";

    private GigaSpace space;

    private String beanName = "memcached";

    private String protocol = "dual";

    private String host;

    private int port;

    private int portRetries = 20;

    private boolean threaded = true;

    private int frameSize = 32768 * 1024;
    private int idleTime;

    private int boundedPort;
    private ServerSocketChannelFactory channelFactory;
    private DefaultChannelGroup allChannels;
    private SpaceCache cache;

    public void setSpace(GigaSpace space) {
        this.space = space;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPortRetries(int portRetries) {
        this.portRetries = portRetries;
    }

    public void setBeanName(String name) {
        this.beanName = name;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setThreaded(boolean threaded) {
        this.threaded = threaded;
    }

    public void afterPropertiesSet() throws Exception {
        cache = new SpaceCache(space);
        channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        allChannels = new DefaultChannelGroup("memcachedChannelGroup");

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new UnifiedProtocolDecoder(cache, allChannels, memcachedVersion, idleTime, false, threaded));
            }
        };
        if ("binary".equalsIgnoreCase(protocol)) {
            pipelineFactory = createMemcachedBinaryPipelineFactory(cache, memcachedVersion, false, idleTime, allChannels);
        } else if ("text".equalsIgnoreCase(protocol)) {
            pipelineFactory = createMemcachedPipelineFactory(cache, memcachedVersion, false, idleTime, frameSize, allChannels);
        }

        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("sendBufferSize", 65536);
        bootstrap.setOption("receiveBufferSize", 65536);

        InetAddress address = null;
        if (host != null) {
            address = InetAddress.getByName(host);
        }

        Exception lastException = null;
        boolean success = false;
        int i;
        for (i = 0; i < portRetries; i++) {
            try {
                Channel serverChannel = bootstrap.bind(new InetSocketAddress(address, port + i));
                allChannels.add(serverChannel);
                success = true;
                break;
            } catch (Exception e) {
                lastException = e;
            }
        }
        if (!success) {
            throw lastException;
        }
        boundedPort = port + i;
        logger.info("memcached started on port [" + boundedPort + "]");
    }

    protected ChannelPipelineFactory createMemcachedBinaryPipelineFactory(
            SpaceCache cache, String memcachedVersion, boolean verbose, int idleTime, DefaultChannelGroup allChannels) {
        return new MemcachedBinaryPipelineFactory(cache, memcachedVersion, verbose, idleTime, allChannels);
    }

    protected ChannelPipelineFactory createMemcachedPipelineFactory(
            SpaceCache cache, String memcachedVersion, boolean verbose, int idleTime, int receiveBufferSize, DefaultChannelGroup allChannels) {
        return new MemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, receiveBufferSize, allChannels);
    }

    public void destroy() throws Exception {
        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();
        if (!future.isCompleteSuccess()) {
            throw new RuntimeException("failure to complete closing all network channels");
        }
        try {
            cache.close();
        } catch (IOException e) {
            throw new RuntimeException("exception while closing storage", e);
        }
        channelFactory.releaseExternalResources();
        logger.info("memcached destroyed");
    }

    public ServiceDetails[] getServicesDetails() {
        return new ServiceDetails[]{new MemcachedServiceDetails(beanName, space.getName(), boundedPort)};
    }

    public ServiceMonitors[] getServicesMonitors() {
        return new ServiceMonitors[]{new MemcachedServiceMonitors(beanName, cache.getGetCmds(), cache.getSetCmds(), cache.getGetHits(), cache.getGetMisses())};
    }
}
