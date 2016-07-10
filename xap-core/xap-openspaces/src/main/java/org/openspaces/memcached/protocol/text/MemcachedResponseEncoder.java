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

package org.openspaces.memcached.protocol.text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.Op;
import org.openspaces.memcached.protocol.ResponseMessage;
import org.openspaces.memcached.protocol.exceptions.ClientException;
import org.openspaces.memcached.util.BufferUtils;

import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.openspaces.memcached.protocol.text.MemcachedPipelineFactory.USASCII;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the
 * StringEncoder
 */
public final class MemcachedResponseEncoder extends SimpleChannelUpstreamHandler {

    protected final static Log logger = LogFactory.getLog(MemcachedResponseEncoder.class);


    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", USASCII);
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", USASCII);
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", USASCII);
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", USASCII);
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", USASCII);
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", USASCII);
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", USASCII);
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", USASCII);
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", USASCII);
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", USASCII);
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", USASCII);

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.
     * Report accordingly.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            throw e.getCause();
        } catch (ClientException ce) {
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(CLIENT_ERROR);
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(ERROR);
        }
    }


    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage command = (ResponseMessage) messageEvent.getMessage();

        Channel channel = messageEvent.getChannel();
        final Op cmd = command.cmd.op;
        switch (cmd) {
            case GET:
            case GETS:
                LocalCacheElement[] results = command.elements;
                int totalBytes = 0;
                for (LocalCacheElement result : results) {
                    if (result != null) {
                        totalBytes += result.size() + 512;
                    }
                }
                ChannelBuffer writeBuffer = ChannelBuffers.dynamicBuffer(totalBytes);

                for (LocalCacheElement result : results) {
                    if (result != null) {
                        writeBuffer.writeBytes(VALUE.duplicate());
                        writeBuffer.writeBytes(result.getKey().bytes);
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeBytes(BufferUtils.itoa(result.getFlags()));
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeBytes(BufferUtils.itoa(result.getData().length));
                        if (cmd == Op.GETS) {
                            writeBuffer.writeByte((byte) ' ');
                            writeBuffer.writeBytes(BufferUtils.ltoa(result.getCasUnique()));
                        }
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                        writeBuffer.writeBytes(result.getData());
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                    }
                }
                writeBuffer.writeBytes(END.duplicate());

                Channels.write(channel, writeBuffer);
                break;
            case SET:
            case CAS:
            case ADD:
            case REPLACE:
            case APPEND:
            case PREPEND:
                if (!command.cmd.noreply)
                    Channels.write(channel, storeResponse(command.response));
                break;
            case INCR:
            case DECR:
                if (!command.cmd.noreply)
                    Channels.write(channel, incrDecrResponseString(command.incrDecrResponse));
                break;

            case DELETE:
                if (!command.cmd.noreply)
                    Channels.write(channel, deleteResponseString(command.deleteResponse));
                break;
            case STATS:
                for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                    for (String statVal : stat.getValue()) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("STAT ");
                        builder.append(stat.getKey());
                        builder.append(" ");
                        builder.append(String.valueOf(statVal));
                        builder.append("\r\n");
                        Channels.write(channel, ChannelBuffers.copiedBuffer(builder.toString(), USASCII));
                    }
                }
                Channels.write(channel, END.duplicate());
                break;
            case VERSION:
                Channels.write(channel, ChannelBuffers.copiedBuffer("VERSION " + command.version + "\r\n", USASCII));
                break;
            case QUIT:
                Channels.disconnect(channel);
                break;
            case FLUSH_ALL:
                if (!command.cmd.noreply) {
                    ChannelBuffer ret = command.flushSuccess ? OK.duplicate() : ERROR.duplicate();

                    Channels.write(channel, ret);
                }
                break;
            default:
                Channels.write(channel, ERROR.duplicate());
                logger.error("error; unrecognized command: " + cmd);
        }

    }

    private ChannelBuffer deleteResponseString(SpaceCache.DeleteResponse deleteResponse) {
        if (deleteResponse == SpaceCache.DeleteResponse.DELETED)
            return DELETED.duplicate();

        return NOT_FOUND.duplicate();
    }


    private ChannelBuffer incrDecrResponseString(Integer ret) {
        if (ret == null)
            return NOT_FOUND.duplicate();

        return ChannelBuffers.copiedBuffer(valueOf(ret) + "\r\n", USASCII);
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace
     * message in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private ChannelBuffer storeResponse(SpaceCache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return EXISTS.duplicate();
            case NOT_FOUND:
                return NOT_FOUND.duplicate();
            case NOT_STORED:
                return NOT_STORED.duplicate();
            case STORED:
                return STORED.duplicate();
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }
}
