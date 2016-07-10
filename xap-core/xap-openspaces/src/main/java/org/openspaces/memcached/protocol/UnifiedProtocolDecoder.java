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

package org.openspaces.memcached.protocol;

import com.j_spaces.kernel.threadpool.DynamicExecutors;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryCommandDecoder;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryResponseEncoder;
import org.openspaces.memcached.protocol.text.MemcachedCommandDecoder;
import org.openspaces.memcached.protocol.text.MemcachedFrameDecoder;
import org.openspaces.memcached.protocol.text.MemcachedResponseEncoder;

import java.util.concurrent.Executors;

/**
 * @author kimchy (shay.banon)
 */
public class UnifiedProtocolDecoder extends FrameDecoder {

    private final SpaceCache cache;

    private final DefaultChannelGroup channelGroup;
    public final String version;

    public final int idle_limit;
    public final boolean verbose;

    private final boolean threaded;

    public UnifiedProtocolDecoder(SpaceCache cache, DefaultChannelGroup channelGroup, String version, int idle_limit, boolean verbose,
                                  boolean threaded) {
        this.cache = cache;
        this.channelGroup = channelGroup;
        this.version = version;
        this.idle_limit = idle_limit;
        this.verbose = verbose;
        this.threaded = threaded;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 1) {
            return null;
        }

        int magic = buffer.getUnsignedByte(buffer.readerIndex());
        if (magic == 0x80) {
            // binary protocol
            ChannelPipeline p = ctx.getPipeline();
            p.addLast("decoder", new MemcachedBinaryCommandDecoder());
            if (threaded) {
                p.addLast("executor", new ExecutionHandler(Executors.newCachedThreadPool(DynamicExecutors.daemonThreadFactory("memcached"))));
            }
            p.addLast("handler", new MemcachedCommandHandler(cache, version, verbose, idle_limit, channelGroup));
            p.addLast("encoder", new MemcachedBinaryResponseEncoder());
            p.remove(this);
        } else {
            SessionStatus status = new SessionStatus().ready();
            ChannelPipeline p = ctx.getPipeline();
            p.addLast("frame", new MemcachedFrameDecoder(status, 32768 * 1024));
            p.addLast("decoder", new MemcachedCommandDecoder(status));
            if (threaded) {
                p.addLast("executor", new ExecutionHandler(Executors.newCachedThreadPool(DynamicExecutors.daemonThreadFactory("memcached"))));
            }
            p.addLast("handler", new MemcachedCommandHandler(cache, version, verbose, idle_limit, channelGroup));
            p.addLast("encoder", new MemcachedResponseEncoder());
            p.remove(this);
        }
        // Forward the current read buffer as is to the new handlers.
        return buffer.readBytes(buffer.readableBytes());
    }
}
