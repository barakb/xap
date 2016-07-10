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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.AbstractSingleFileConfirmationHolder;
import com.gigaspaces.internal.utils.StringUtils;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileConfirmationHolder extends AbstractSingleFileConfirmationHolder {
    private final long[] _bucketLastConfirmedKeys;
    private long _globalLastConfirmedKey = -1;
    private boolean _hadAnyHandshake = false;
    private final SortedSet<Long> _globalLastConfirmedKeysWindow = new TreeSet<Long>();

    public MultiBucketSingleFileConfirmationHolder(long[] bucketLastConfirmedKeys) {
        _bucketLastConfirmedKeys = bucketLastConfirmedKeys;
    }

    public long[] getBucketLastConfirmedKeys() {
        return _bucketLastConfirmedKeys;
    }

    public long getGlobalLastConfirmedKey() {
        return _globalLastConfirmedKey;
    }

    @Override
    public long getLastConfirmedKey() {
        return getGlobalLastConfirmedKey();
    }

    public boolean hadAnyHandshake() {
        return _hadAnyHandshake;
    }

    public long overrideGlobalLastConfirmedKey(long globalLastConfirmedKey) {
        _hadAnyHandshake = true;
        _globalLastConfirmedKey = globalLastConfirmedKey;
        long nextConsecutiveGlobalKey = _globalLastConfirmedKey + 1;

        for (Iterator<Long> iterator = _globalLastConfirmedKeysWindow.iterator(); iterator.hasNext(); ) {
            Long globalConfirmedKey = iterator.next();
            if (globalConfirmedKey <= _globalLastConfirmedKey) {
                iterator.remove();
            } else if (globalConfirmedKey == nextConsecutiveGlobalKey) {
                iterator.remove();
                _globalLastConfirmedKey = globalConfirmedKey;
                nextConsecutiveGlobalKey++;
            } else
                break;
        }
        return _globalLastConfirmedKey;
    }

    public void overrideBucketKeys(long[] bucketNextKeys) {
        for (int i = 0; i < bucketNextKeys.length; i++) {
            _bucketLastConfirmedKeys[i] = bucketNextKeys[i] - 1;
        }
    }

    public long addGlobalKeyConfirmed(long globalKey) {
        _hadAnyHandshake = true;
        //If the window is empty and the global key is in consecutive order, we can update the global last confirmed key
        long nextConsecutiveGlobalKey = _globalLastConfirmedKey + 1;
        if (_globalLastConfirmedKeysWindow.isEmpty() && globalKey == nextConsecutiveGlobalKey)
            _globalLastConfirmedKey = globalKey;
        else {
            _globalLastConfirmedKeysWindow.add(globalKey);
            for (Iterator<Long> iterator = _globalLastConfirmedKeysWindow.iterator(); iterator.hasNext(); ) {
                Long globalConfirmedKey = iterator.next();
                if (globalConfirmedKey <= nextConsecutiveGlobalKey) {
                    iterator.remove();
                    //If the above is not true this is a duplicate confirmation we have received so we should not increase
                    //the nextConsequetiveGlobalKey
                    if (globalConfirmedKey == nextConsecutiveGlobalKey) {
                        _globalLastConfirmedKey = Math.max(globalConfirmedKey, nextConsecutiveGlobalKey);
                        nextConsecutiveGlobalKey++;
                    }
                } else
                    break;
            }
        }
        return _globalLastConfirmedKey;
    }

    @Override
    public String toString() {
        return "ConfirmationHolder [_bucketLastConfirmedKeys=" + StringUtils.NEW_LINE
                + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_bucketLastConfirmedKeys)
                + ", _globalLastConfirmedKey=" + _globalLastConfirmedKey
                + ", _globalLastConfirmedKeysWindow="
                + _globalLastConfirmedKeysWindow + "]";
    }

}
