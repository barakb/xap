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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.openspaces.memcached.Key;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.CommandMessage;
import org.openspaces.memcached.protocol.Op;
import org.openspaces.memcached.protocol.SessionStatus;
import org.openspaces.memcached.protocol.exceptions.InvalidProtocolStateException;
import org.openspaces.memcached.protocol.exceptions.MalformedCommandException;
import org.openspaces.memcached.protocol.exceptions.UnknownCommandException;
import org.openspaces.memcached.util.BufferUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The MemcachedCommandDecoder is responsible for taking lines from the MemcachedFrameDecoder and
 * parsing them into CommandMessage instances for handling by the MemcachedCommandHandler <p/>
 * Protocol status is held in the SessionStatus instance which is shared between each of the
 * decoders in the pipeline.
 */
public final class MemcachedCommandDecoder extends SimpleChannelUpstreamHandler {

    private SessionStatus status;

    private static final byte[] NOREPLY = "noreply".getBytes();


    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }

    /**
     * Process an inbound string from the pipeline's downstream, and depending on the state (waiting
     * for data or processing commands), turn them into the correct type of command.
     *
     * @param channelHandlerContext netty channel handler context
     * @param messageEvent          the netty event that corresponds to the message
     */
    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ChannelBuffer in = (ChannelBuffer) messageEvent.getMessage();

        try {
            // Because of the frame handler, we are assured that we are receiving only complete lines or payloads.
            // Verify that we are in 'processing()' mode
            if (status.state == SessionStatus.State.PROCESSING) {
                // split into pieces
                List<byte[]> pieces = new ArrayList<byte[]>(6);
                int pos = in.bytesBefore((byte) ' ');
                do {
                    if (pos != -1) {
                        byte[] piece = new byte[pos];
                        in.readBytes(piece);
                        pieces.add(piece);
                        in.skipBytes(1);
                    }
                } while ((pos = in.bytesBefore((byte) ' ')) != -1);
                byte[] remainder = new byte[in.readableBytes()];
                in.readBytes(remainder);
                pieces.add(remainder);

                processLine(pieces, messageEvent.getChannel(), channelHandlerContext);
            } else if (status.state == SessionStatus.State.PROCESSING_MULTILINE) {
                ChannelBuffer slice = in.copy();
                byte[] payload = slice.array();
                in.skipBytes(in.readableBytes());
                continueSet(messageEvent.getChannel(), status, payload, channelHandlerContext);
            } else {
                throw new InvalidProtocolStateException("invalid protocol state");
            }

        } finally {
            // Now indicate that we need more for this command by changing the session status's state.
            // This instructs the frame decoder to start collecting data for us.
            // Note, we don't do this if we're waiting for data.
            if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();
        }
    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by
     * the session handler, or (in the case of SET-type commands) partially parses the command and
     * sets the session into a state to wait for additional data.
     *
     * @param parts                 the (originally space separated) parts of the command
     * @param channel               the netty channel to operate on
     * @param channelHandlerContext the netty channel handler context
     */
    private void processLine(List<byte[]> parts, Channel channel, ChannelHandlerContext channelHandlerContext) throws UnknownCommandException, MalformedCommandException {
        final int numParts = parts.size();

        // Turn the command into an enum for matching on
        Op op;
        try {
            op = Op.findOp(parts.get(0));
        } catch (IllegalArgumentException e) {
            throw new UnknownCommandException("unknown operation: " + new String(parts.get(0)));
        }

        // Produce the initial command message, for filling in later
        CommandMessage cmd = CommandMessage.command(op);

        switch (op) {
            case DELETE: // delete <key> [<time>] [noreply]\r\n
                if (numParts < 2 || numParts > 4) // Malformed
                    throw new MalformedCommandException("invalid delete command");

                cmd.setKey(parts.get(1));

                if (numParts > 2) {
                    if (Arrays.equals(parts.get(numParts - 1), NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 4) // else --> delete <key> [noreply]
                            cmd.time = BufferUtils.atoi((parts.get(2)));
                    } else if (numParts == 3) { // delete <key> [<time>]
                        cmd.time = BufferUtils.atoi((parts.get(2)));
                    } else {
                        throw new MalformedCommandException("invalid delete command");
                    }
                }
                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;

            case DECR: // decr <key> <value> [noreply]\r\n
            case INCR: // incr <key> <value> [noreply]\r\n
                if (numParts < 3 || numParts > 4) // Malformed
                    throw new MalformedCommandException("invalid incr/decr command");

                cmd.setKey(parts.get(1));
                cmd.incrAmount = BufferUtils.atoi(parts.get(2));

                if (numParts == 4 && Arrays.equals(parts.get(3), NOREPLY)) {
                    cmd.noreply = true;
                }

                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;

            case FLUSH_ALL: // flush_all [time] [noreply]\r\n
                if (numParts >= 1) {
                    if (Arrays.equals(parts.get(numParts - 1), NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 3)
                            cmd.time = BufferUtils.atoi((parts.get(1)));
                    } else if (numParts == 2)
                        cmd.time = BufferUtils.atoi((parts.get(1)));
                }
                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;
            case VERBOSITY: // verbosity <time> [noreply]\r\n
                // Malformed
                if (numParts < 2 || numParts > 3)
                    throw new MalformedCommandException("invalid verbosity command");

                cmd.time = BufferUtils.atoi((parts.get(1))); // verbose level

                if (numParts > 1 && Arrays.equals(parts.get(2), NOREPLY))
                    cmd.noreply = true;
                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;
            case APPEND: // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
            case PREPEND:
            case REPLACE:
            case ADD:
            case SET:
            case CAS: // cas <key> <flags> <exptime> <bytes> <cas unqiue> [noreply]\r\n
                if (numParts < 5) { //malformed
                    throw new MalformedCommandException("invalid command length");
                }

                // Fill in all the elements of the command
                int size = BufferUtils.atoi(parts.get(4));
                int expire = BufferUtils.atoi(parts.get(3));
                int flags = BufferUtils.atoi(parts.get(2));
                // expire is relative to now, always, translates to LEASE.
                if (expire == 0) {
                    expire = Integer.MAX_VALUE;
                } else if (expire > SpaceCache.THIRTY_DAYS) {
                    expire = LocalCacheElement.Now() - expire;
                }

                cmd.element = new LocalCacheElement(new Key(parts.get(1)), flags, expire, 0L);

                // look for cas and "noreply" elements
                if (numParts > 5) {
                    int noreply = op == Op.CAS ? 6 : 5;
                    if (op == Op.CAS) {
                        cmd.cas_key = BufferUtils.atol(parts.get(5));
                    }

                    if (numParts == noreply + 1 && Arrays.equals(parts.get(noreply), NOREPLY))
                        cmd.noreply = true;
                }

                // Now indicate that we need more for this command by changing the session status's state.
                // This instructs the frame decoder to start collecting data for us.
                status.needMore(size, cmd);
                break;
            case GET: //get <key>*\r\n
            case GETS: //gets <key>*\r\n
            case STATS: // stats [args]\r\n
                // Get all the keys
                cmd.setKeys(parts.subList(1, numParts));

                // Pass it on.
                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;
            case VERSION: // version\r\n
            case QUIT: // quit\r\n
                if (numParts > 1) { //malformed
                    throw new MalformedCommandException("invalid command length");
                }
                Channels.fireMessageReceived(channelHandlerContext, cmd, channel.getRemoteAddress());
                break;
            default:
                throw new UnknownCommandException("unknown command: " + op);
        }
    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param channel               netty channel
     * @param state                 the current session status (unused)
     * @param remainder             the bytes picked up
     * @param channelHandlerContext netty channel handler context
     */
    private void continueSet(Channel channel, SessionStatus state, byte[] remainder, ChannelHandlerContext channelHandlerContext) {
        state.cmd.element.setData(remainder);
        Channels.fireMessageReceived(channelHandlerContext, state.cmd, channelHandlerContext.getChannel().getRemoteAddress());
    }
}
