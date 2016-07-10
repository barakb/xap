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

import org.openspaces.memcached.Key;
import org.openspaces.memcached.LocalCacheElement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The payload object holding the parsed message.
 */
public final class CommandMessage implements Serializable {

    private static final long serialVersionUID = 1663628151791997691L;

    public static enum ErrorType {
        OK, ERROR, CLIENT_ERROR
    }

    final public Op op;
    public LocalCacheElement element;
    public List<Key> keys;
    public boolean noreply;
    public long cas_key;
    public int time = 0;
    public ErrorType error = ErrorType.OK;
    public String errorString;
    public int opaque;
    public boolean addKeyToResponse = false;

    public Integer incrDefault;
    public int incrExpiry;
    public int incrAmount;

    private CommandMessage(Op op) {
        this.op = op;
        element = null;
        keys = new ArrayList<Key>();
    }

    public void setKey(byte[] key) {
        this.keys = new ArrayList<Key>();
        this.keys.add(new Key(key));
    }

    public void setKeys(Iterable<byte[]> keys) {
        this.keys = new ArrayList<Key>();
        for (byte[] key : keys) {
            this.keys.add(new Key(key));
        }
    }

    public static CommandMessage error(String errorString) {
        CommandMessage errcmd = new CommandMessage(null);
        errcmd.error = ErrorType.ERROR;
        errcmd.errorString = errorString;
        return errcmd;
    }

    public static CommandMessage clientError(String errorString) {
        CommandMessage errcmd = new CommandMessage(null);
        errcmd.error = ErrorType.CLIENT_ERROR;
        errcmd.errorString = errorString;
        return errcmd;
    }

    public static CommandMessage command(Op operation) {
        return new CommandMessage(operation);
    }
}