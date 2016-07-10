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

package com.gigaspaces.internal.serialization;

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
//import com.gigaspaces.internal.io.MarshalInputStream;
//import com.gigaspaces.internal.io.MarshalOutputStream;

@com.gigaspaces.api.InternalApi
public class HashMapSerializer implements IClassSerializer<Map<String, Object>> {
    private static final int OptimizeLevel = 0;

    public byte getCode() {
        return CODE_HASHMAP;
    }

    public Map<String, Object> read(ObjectInput in) throws IOException, ClassNotFoundException {
        /* Experimental code
        if (OptimizeLevel == 2)
		{
			final MarshalInputStream mis = (MarshalInputStream) in;
			
			int size = in.readInt();
			Map<String, Object> map = new HashMap<String, Object>();
			for (int i=0 ; i < size ; i++)
			{
				String key = mis.readNivObject();
				Object value = IOUtils.readObject(in);
				map.put(key, value);
			}
			return map;
		}
		else*/
        if (OptimizeLevel == 1) {
            int size = in.readInt();
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 0; i < size; i++) {
                String key = IOUtils.readString(in);
                Object value = IOUtils.readObject(in);
                map.put(key, value);
            }
            return map;
        } else
            return (Map<String, Object>) in.readObject();
    }

    public void write(ObjectOutput out, Map<String, Object> obj)
            throws IOException {
		/* Experimental code
		if (OptimizeLevel == 2)
		{
			final MarshalOutputStream mos = (MarshalOutputStream) out;

			int size = obj.size();
			out.writeInt(size);
			
			for (Entry<String, Object> entry : obj.entrySet())
			{
				mos.writeNivObject(entry.getKey());
				IOUtils.writeObject(out, entry.getValue());
			}
		}
		else */
        if (OptimizeLevel == 1) {
            int size = obj.size();
            out.writeInt(size);

            for (Entry<String, Object> entry : obj.entrySet()) {
                IOUtils.writeString(out, entry.getKey());
                IOUtils.writeObject(out, entry.getValue());
            }
        } else {
            out.writeObject(obj);
        }
    }
}
