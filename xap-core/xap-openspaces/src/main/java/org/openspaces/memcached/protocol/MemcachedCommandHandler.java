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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.openspaces.memcached.Key;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.exceptions.UnknownCommandException;

import java.util.concurrent.atomic.AtomicInteger;

// TODO implement flush_all delay

/**
 * The actual command handler, which is responsible for processing the CommandMessage instances that
 * are inbound from the protocol decoders. <p/> One instance is shared among the entire pipeline,
 * since this handler is stateless, apart from some globals for the entire daemon. <p/> The command
 * handler produces ResponseMessages which are destined for the response encoder.
 */
@ChannelHandler.Sharable
public final class MemcachedCommandHandler extends SimpleChannelUpstreamHandler {

    protected final static Log logger = LogFactory.getLog(MemcachedCommandHandler.class);

    public final AtomicInteger curr_conns = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();

    /**
     * The following state variables are universal for the entire daemon. These are used for
     * statistics gathering. In order for these values to work properly, the handler _must_ be
     * declared with a ChannelPipelineCoverage of "all".
     */
    public final String version;

    public final int idle_limit;
    public final boolean verbose;


    /**
     * The actual physical data storage.
     */
    private final SpaceCache cache;

    /**
     * The channel group for the entire daemon, used for handling global cleanup on shutdown.
     */
    private final DefaultChannelGroup channelGroup;

    /**
     * Construct the server session handler
     *
     * @param cache            the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     */
    public MemcachedCommandHandler(SpaceCache cache, String memcachedVersion, boolean verbosity, int idle, DefaultChannelGroup channelGroup) {
        this.cache = cache;

        version = memcachedVersion;
        verbose = verbosity;
        idle_limit = idle;
        this.channelGroup = channelGroup;
    }


    /**
     * On open we manage some statistics, and add this connection to the channel group.
     */
    @Override
    public void channelOpen(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
        channelGroup.add(channelHandlerContext.getChannel());
    }

    /**
     * On close we manage some statistics, and remove this connection from the channel group.
     */
    @Override
    public void channelClosed(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        curr_conns.decrementAndGet();
        channelGroup.remove(channelHandlerContext.getChannel());
    }


    /**
     * The actual meat of the matter.  Turn CommandMessages into executions against the physical
     * cache, and then pass on the downstream messages.
     */

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        if (!(messageEvent.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            channelHandlerContext.sendUpstream(messageEvent);
            return;
        }

        CommandMessage command = (CommandMessage) messageEvent.getMessage();
        Op cmd = command.op;
        int cmdKeysSize = command.keys.size();

        // first process any messages in the delete queue
        cache.asyncEventPing();

        // now do the real work
        if (this.verbose) {
            StringBuilder log = new StringBuilder();
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.getKey());
            }
            for (int i = 0; i < cmdKeysSize; i++) {
                log.append(" ").append(command.keys.get(i));
            }
            logger.info(log.toString());
        }

        Channel channel = messageEvent.getChannel();
        if (cmd == Op.GET || cmd == Op.GETS) {
            handleGets(channelHandlerContext, command, channel);
        } else if (cmd == Op.SET) {
            handleSet(channelHandlerContext, command, channel);
        } else if (cmd == Op.CAS) {
            handleCas(channelHandlerContext, command, channel);
        } else if (cmd == Op.ADD) {
            handleAdd(channelHandlerContext, command, channel);
        } else if (cmd == Op.REPLACE) {
            handleReplace(channelHandlerContext, command, channel);
        } else if (cmd == Op.APPEND) {
            handleAppend(channelHandlerContext, command, channel);
        } else if (cmd == Op.PREPEND) {
            handlePrepend(channelHandlerContext, command, channel);
        } else if (cmd == Op.INCR) {
            handleIncr(channelHandlerContext, command, channel);
        } else if (cmd == Op.DECR) {
            handleDecr(channelHandlerContext, command, channel);
        } else if (cmd == Op.DELETE) {
            handleDelete(channelHandlerContext, command, channel);
        } else if (cmd == Op.STATS) {
            handleStats(channelHandlerContext, command, cmdKeysSize, channel);
        } else if (cmd == Op.VERSION) {
            handleVersion(channelHandlerContext, command, channel);
        } else if (cmd == Op.QUIT) {
            handleQuit(channel);
        } else if (cmd == Op.FLUSH_ALL) {
            handleFlush(channelHandlerContext, command, channel);
        } else if (cmd == Op.VERBOSITY) {
            handleVerbosity(channelHandlerContext, command, channel);
        } else if (cmd == null) {
            // NOOP
            handleNoOp(channelHandlerContext, command);
        } else {
            throw new UnknownCommandException("unknown command:" + cmd);

        }

    }

    protected void handleNoOp(ChannelHandlerContext channelHandlerContext, CommandMessage command) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command));
    }

    protected void handleFlush(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withFlushResponse(cache.flush_all(command.time)), channel.getRemoteAddress());
    }

    protected void handleVerbosity(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command), channel.getRemoteAddress());
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = version;
        Channels.fireMessageReceived(channelHandlerContext, responseMessage, channel.getRemoteAddress());
    }

    protected void handleStats(ChannelHandlerContext channelHandlerContext, CommandMessage command, int cmdKeysSize, Channel channel) {
        String option = "";
        if (cmdKeysSize > 0) {
            option = new String(command.keys.get(0).bytes);
        }
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withStatResponse(cache.stat(option)), channel.getRemoteAddress());
    }

    protected void handleDelete(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withDeleteResponse(dr), channel.getRemoteAddress());
    }

    protected void handleDecr(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handleIncr(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withIncrDecrResponse(incrDecrResp), channel.getRemoteAddress());
    }

    protected void handlePrepend(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.prepend(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAppend(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.append(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleReplace(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.replace(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleAdd(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.add(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleCas(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.cas(command.cas_key, command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleSet(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        SpaceCache.StoreResponse ret;
        ret = cache.set(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext channelHandlerContext, CommandMessage command, Channel channel) {
        Key[] keys = new Key[command.keys.size()];
        keys = command.keys.toArray(keys);
        LocalCacheElement[] results = get(keys);
        ResponseMessage resp = new ResponseMessage(command).withElements(results);
        Channels.fireMessageReceived(channelHandlerContext, resp, channel.getRemoteAddress());
    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private LocalCacheElement[] get(Key... keys) {
        return cache.get(keys);
    }

}