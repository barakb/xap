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

package com.j_spaces.core.client.cache.map;

import com.j_spaces.javax.cache.CacheEntry;

/**
 * Holds the entry and all the Statics.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 4.5
 */
public class BaseCacheEntry implements CacheEntry {
    //private EntryStatics           _statics;
    private Object _key;
    private Object _value;
    private int _version; // TODO move to long

    /**
     * Constructor.
     */
    public BaseCacheEntry(Object key, Object value, long timeToLive, int version) {
        // TODO support timeToLive
        //  _statics    = new EntryStatics( timeToLive);
        _value = value;
        _version = version;
        _key = key;
    }

//   public int getHits() {
//      return _statics._hits;
//   }
//   
//   public long getLastAccessTime() {
//      return _statics._lastAccessTime;
//   }
//   
//   public long getLastUpdateTime() {
//      return _statics._lastUpdateTime;
//   }
//   
//   public long getCreationTime() {
//      return _statics._creationTime;
//   }
//   
//   public long getExpirationTime() {
//      return _statics._expirationTime;
//   }
//  

    /**
     * {@inheritDoc}
     */
    public long getCacheEntryVersion() {
        return _version;
    }
//   
//   public boolean isValid() {
//      return _statics._valid;
//   }
//   
//   public long getCost() {
//      return _statics._cost;
//   }

    /**
     * {@inheritDoc}
     */
    public Object getValue() {
        return _value;
    }

    /**
     * {@inheritDoc}
     */
    public Object getKey() {
        return _key;
    }

    /**
     * {@inheritDoc}
     */
    public Object setValue(Object newValue) {
        Object old = newValue;
        _value = newValue;
        return old;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return _key.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DummyCacheEntry) {
            // tries the other way may be VersionCacheEntry.
            return obj.equals(this);
        }

        if (obj == this) {
            return true;
        }

        CacheEntry entry = (CacheEntry) obj;
        return _key.equals(entry.getKey()) && _value.equals(entry.getValue());
    }

    /**
     * Holds the Entry Statics.
     * @author Guy Korland
     */
//   private class EntryStatics{
//      
//      public int        _hits = 0;
//      
//      public long       _lastAccessTime;
//      public long       _lastUpdateTime;
//      public long       _creationTime;
//      public long       _expirationTime;
//      public boolean    _valid = true;
//      public long       _cost;
//      
//      public EntryStatics( long timeToLive){
//         _creationTime  = SystemTime.timeMillis();
//         _lastAccessTime = _creationTime;
//         _lastUpdateTime = _creationTime; 
//         _expirationTime = _creationTime + timeToLive;
//      }   
//   }   
}
