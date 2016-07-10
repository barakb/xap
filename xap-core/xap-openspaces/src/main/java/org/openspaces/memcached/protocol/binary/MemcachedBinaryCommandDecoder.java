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

package org.openspaces.memcached.protocol.binary;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.openspaces.memcached.Key;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.CommandMessage;
import org.openspaces.memcached.protocol.Op;
import org.openspaces.memcached.protocol.exceptions.MalformedCommandException;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 */
@ChannelHandler.Sharable
public class MemcachedBinaryCommandDecoder extends FrameDecoder {

    public static final Charset USASCII = Charset.forName("US-ASCII");

    public static enum BinaryOp {
        Get(0x00, Op.GET, false),
        Set(0x01, Op.SET, false),
        Add(0x02, Op.ADD, false),
        Replace(0x03, Op.REPLACE, false),
        Delete(0x04, Op.DELETE, false),
        Increment(0x05, Op.INCR, false),
        Decrement(0x06, Op.DECR, false),
        Quit(0x07, Op.QUIT, false),
        Flush(0x08, Op.FLUSH_ALL, false),
        GetQ(0x09, Op.GET, false),
        Noop(0x0A, null, false),
        Version(0x0B, Op.VERSION, false),
        GetK(0x0C, Op.GET, false, true),
        GetKQ(0x0D, Op.GET, true, true),
        Append(0x0E, Op.APPEND, false),
        Prepend(0x0F, Op.PREPEND, false),
        Stat(0x10, Op.STATS, false),
        SetQ(0x11, Op.SET, true),
        AddQ(0x12, Op.ADD, true),
        ReplaceQ(0x13, Op.REPLACE, true),
        DeleteQ(0x14, Op.DELETE, true),
        IncrementQ(0x15, Op.INCR, true),
        DecrementQ(0x16, Op.DECR, true),
        QuitQ(0x17, Op.QUIT, true),
        FlushQ(0x18, Op.FLUSH_ALL, true),
        AppendQ(0x19, Op.APPEND, true),
        PrependQ(0x1A, Op.PREPEND, true);

        final private byte code;
        final private Op correspondingOp;
        final private boolean noreply;
        final private boolean addKeyToResponse;

        BinaryOp(int code, Op correspondingOp, boolean noreply) {
            this.code = (byte) code;
            this.correspondingOp = correspondingOp;
            this.noreply = noreply;
            this.addKeyToResponse = false;
        }

        BinaryOp(int code, Op correspondingOp, boolean noreply, boolean addKeyToResponse) {
            this.code = (byte) code;
            this.correspondingOp = correspondingOp;
            this.noreply = noreply;
            this.addKeyToResponse = addKeyToResponse;
        }

        public static BinaryOp forCommandMessage(CommandMessage msg) {
            if (Op.CAS == msg.op) // CAS is a special case since it has no opcode but uses the Set opcode
            {
                if (Set.noreply == msg.noreply && Set.addKeyToResponse == msg.addKeyToResponse)
                    return Set;
            } else {
                for (BinaryOp binaryOp : values()) {
                    if (binaryOp.correspondingOp == msg.op && binaryOp.noreply == msg.noreply && binaryOp.addKeyToResponse == msg.addKeyToResponse) {
                        return binaryOp;
                    }
                }
            }
            return null;
        }

        public boolean isAddKeyToResponse() {
            return addKeyToResponse;
        }

        public boolean isNoreply() {
            return noreply;
        }

        public Op getCorrespondingOp() {
            return correspondingOp;
        }

        public byte getCode() {
            return code;
        }

    }

    @Override
    protected Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, ChannelBuffer channelBuffer) throws Exception {

        // need at least 24 bytes, to get header
        if (channelBuffer.readableBytes() < 24) return null;

        // get the header
        channelBuffer.markReaderIndex();
        ChannelBuffer headerBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 24);
        channelBuffer.readBytes(headerBuffer);

        short magic = headerBuffer.readUnsignedByte();

        // magic should be 0x80
        if (magic != 0x80) {
            headerBuffer.resetReaderIndex();

            throw new MalformedCommandException("binary request payload is invalid, magic byte incorrect");
        }

        short opcode = headerBuffer.readUnsignedByte();
        short keyLength = headerBuffer.readShort();
        short extraLength = headerBuffer.readUnsignedByte();
        short dataType = headerBuffer.readUnsignedByte();   // unused
        short reserved = headerBuffer.readShort(); // unused
        int totalBodyLength = headerBuffer.readInt();
        int opaque = headerBuffer.readInt();
        long cas = headerBuffer.readLong();

        // we want the whole of totalBodyLength; otherwise, keep waiting.
        if (channelBuffer.readableBytes() < totalBodyLength) {
            channelBuffer.resetReaderIndex();
            return null;
        }

        // This assumes correct order in the enum. If that ever changes, we will have to scan for 'code' field.
        BinaryOp bcmd = BinaryOp.values()[opcode];

        Op cmdType = bcmd == BinaryOp.Set && cas != 0 ? Op.CAS : bcmd.getCorrespondingOp();
        CommandMessage cmdMessage = CommandMessage.command(cmdType);
        cmdMessage.noreply = bcmd.isNoreply();
        cmdMessage.cas_key = cas;
        cmdMessage.opaque = opaque;
        cmdMessage.addKeyToResponse = bcmd.isAddKeyToResponse();

        // get extras. could be empty.
        ChannelBuffer extrasBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, extraLength);
        channelBuffer.readBytes(extrasBuffer);

        // get the key if any
        if (keyLength != 0) {
            ChannelBuffer keyBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, keyLength);
            channelBuffer.readBytes(keyBuffer);

            ArrayList<Key> keys = new ArrayList<Key>();
            byte[] key = keyBuffer.array();
            keys.add(new Key(key));

            cmdMessage.keys = keys;


            if (cmdType == Op.ADD ||
                    cmdType == Op.SET ||
                    cmdType == Op.REPLACE ||
                    cmdType == Op.APPEND ||
                    cmdType == Op.PREPEND ||
                    cmdType == Op.CAS) {
                // TODO these are backwards from the spec, but seem to be what spymemcached demands -- which has the mistake?!
                int expire = (short) (extrasBuffer.capacity() != 0 ? extrasBuffer.readUnsignedShort() : 0);
                short flags = (short) (extrasBuffer.capacity() != 0 ? extrasBuffer.readUnsignedShort() : 0);

                // the remainder of the message -- that is, totalLength - (keyLength + extraLength) should be the payload
                int size = totalBodyLength - keyLength - extraLength;

                // expire is relative to now, always, translates to LEASE.
                if (expire == 0) {
                    expire = Integer.MAX_VALUE;
                } else if (expire > SpaceCache.THIRTY_DAYS) {
                    expire = LocalCacheElement.Now() - expire;
                }

                cmdMessage.element = new LocalCacheElement(new Key(key), flags, expire, 0L);
                cmdMessage.element.setData(new byte[size]);
                channelBuffer.readBytes(cmdMessage.element.getData(), 0, size);
            } else if (cmdType == Op.INCR || cmdType == Op.DECR) {
                long initialValue = extrasBuffer.readUnsignedInt();
                long amount = extrasBuffer.readUnsignedInt();
                long expiration = extrasBuffer.readUnsignedInt();

                cmdMessage.incrAmount = (int) amount;
                cmdMessage.incrDefault = (int) initialValue;
                cmdMessage.incrExpiry = (int) expiration;
            }
        }

        return cmdMessage;
    }
}
