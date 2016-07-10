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


package com.j_spaces.core.client;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.core.EntrySerializationException;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.IJSpace;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;

/**
 * @deprecated Since 8.0 - This class has been deprecated and will be removed in future versions.
 * For id-based queries, use the GigaSpace.readById/readByIds and related overloades. For extended
 * matching, use SQLQuery. For weak-type entries, use SpaceDocument.
 */
@Deprecated

public class ExternalEntry implements Entry, IGSEntry, Cloneable {
    private static final long serialVersionUID = 1L;
    /**
     * <pre>
     * Store Entry Unique ID.
     * If this field is not <B>null</B> then this UID will be used
     * by the Space, otherwise the space will generate it automatically.
     * When entry have all its fields null (null template) and its UID is assigned,
     * matching will be done using the UID only.
     *
     * The UID is a String based identifier and composed of the following parts:
     * - Class information class Hashcode and name size
     * - Space node name At clustered environment combined from container-name :space name. At
     * non-clustered environment combined from dummy name.
     * - Timestamp
     * - Counter
     * </pre>
     */
    public String m_UID;

    /**
     * The Entry class name.
     */
    public String m_ClassName;

    /**
     * The Entry super classes names array. The order of the values in the array is the class
     * inheritance hierarchy.
     */
    public String[] m_SuperClassesNames;

    /**
     * The Entry field names array.
     */
    public String[] m_FieldsNames;

    /**
     * The Entry field Types.
     */
    public String[] m_FieldsTypes;

    /**
     * The Entry field values.
     */
    public Object[] m_FieldsValues;

    /**
     * Contains array of boolean values that indicate which of the class fields are index fields.
     */
    public boolean[] m_IndexIndicators;

    /**
     * The field name representing the primary key. Usually the field name of the first indexed
     * field.
     *
     * @see #getPrimaryKeyName()
     */
    public String m_PrimaryKeyName;

    /**
     * Contains a tag that indicates whether the class should be replicated or not. Used only in the
     * case of partial replication.
     */
    public boolean m_Replicatable;

    /**
     * Contains array of UIDs. Used in readMultiple/takeMultiple operations. holds the UIDs of the
     * Objects that are going to be read or take. when using m_ReturnOnlyUids=<B>true</B> the
     * returned UIDs are are kept here in the first Entry returned.
     */
    public String[] m_MultipleUIDs;

    /**
     * If true readMultiple/takeMultiple return only UIDs.
     */
    public boolean m_ReturnOnlyUids;

    /**
     * FIFO Indication, if <code>true</code> the entry will be returned in a FIFO way.
     */
    public boolean m_isFifo;

    /**
     * Contains a version number that is incremented each time the entry is updated. The initial
     * value to be stored in the space is 1, and is incremented after each update. Value is used for
     * optimistic locking.
     */
    public int m_VersionID;

    /**
     * Codes for extending matching.
     *
     * @see #setExtendedMatchCodes(short[])
     */
    public short[] m_ExtendedMatchCodes;

    /**
     * range values- correspond to m_ExtendedMatchCodes, this is UP-TO and include values.
     */
    public Object[] m_RangeValues;

    /**
     * boolean array that indicates for each range value if it is included in range or not
     */
    public boolean[] m_RangeValuesInclusion;


    /**
     * Read only field. Time (in milliseconds) left for this entry to live. This value is correct
     * for the operation time.
     */
    public long m_TimeToLive = Lease.FOREVER;

    /**
     * If <code>true</code> will be transient.
     */
    public boolean m_isTransient;

    /**
     * If true  Lease object would not return from the write/writeMultiple operations.
     **/
    public boolean m_NOWriteLeaseMode;

    /**
     * The field name representing routing field name. on that is taken as the first index for
     * partition.
     *
     * @see #getRoutingFieldName()
     */
    public String routingFieldName;
    public boolean[] _primitiveFields;

    public boolean _returnTrueType;

    /**
     * Default constructor required for <code>java.io.Externalizable</code> interface.
     */
    public ExternalEntry() {
    }

    /**
     * Constructs an ExternalEntry object that will be used as a template. The template matching
     * will be done based on the entry UId only.
     *
     * @param entryUID entry UID.
     */
    public ExternalEntry(String entryUID)    // operate on entry by this UID
    {
        m_UID = entryUID;
    }

    /**
     * Copy constructor, creates a copy of this entry.
     *
     * @param entry another ExternalEntry
     */
    protected ExternalEntry(ExternalEntry entry) {
        m_UID = entry.m_UID;
        m_ClassName = entry.m_ClassName;
        m_SuperClassesNames = entry.m_SuperClassesNames;
        m_FieldsNames = entry.m_FieldsNames;
        m_FieldsTypes = entry.m_FieldsTypes;
        m_FieldsValues = entry.m_FieldsValues;
        m_IndexIndicators = entry.m_IndexIndicators;
        m_PrimaryKeyName = entry.m_PrimaryKeyName;
        m_Replicatable = entry.m_Replicatable;
        m_MultipleUIDs = entry.m_MultipleUIDs;
        m_ReturnOnlyUids = entry.m_ReturnOnlyUids;
        m_isFifo = entry.m_isFifo;
        m_VersionID = entry.m_VersionID;
        m_ExtendedMatchCodes = entry.m_ExtendedMatchCodes;
        m_RangeValues = entry.m_RangeValues;
        m_RangeValuesInclusion = entry.m_RangeValuesInclusion;
        m_TimeToLive = entry.m_TimeToLive;
        m_isTransient = entry.m_isTransient;
        m_NOWriteLeaseMode = entry.m_NOWriteLeaseMode;
        routingFieldName = entry.routingFieldName;
    }

    /**
     * Constructs an ExternalEntry object that will be used as a template for
     * readMultiple/takeMultiple operations.<br>
     *
     * You can read/take multiple Entries from the space using their UID's in one space operation.
     * You should construct ExternalEntry template that includes array of the Entry's UID and use
     * this template with the readMultiple operation. See below example: <br> The Entry Class:
     * <pre>
     * <code>
     *
     * public class MyEntry extends MetaDataEntry{
     * public MyEntry (){}
     * public MyEntry (int num)
     * {
     * this.attr1 = "attr1 " + num;
     * this.attr2 = "attr2 " + num;
     * }
     * public String attr1,attr2;
     * public String toString()
     * {
     * return "UID:"  + __getEntryInfo().m_UID + " attr1:" + attr1 + " attr2:"+ attr2;
     * }
     * }
     * </code>
     * </pre>
     * The Application code:
     * <pre>
     * <code>
     * IJSpace space = (IJSpace )SpaceFinder.find("/./mySpace");
     * String uid[] = new String[10];
     * for (int i=0; i < 10 ;i++ )
     * {
     * uid[i] = ClientUIDHandler.createUIDFromName(i , MyEntry.class.getName());
     * MyEntry entry = new MyEntry(i);
     * entry.__setEntryInfo(new EntryInfo(uid[i],0));
     * space.write(entry , null ,Lease.FOREVER );
     * }
     * ExternalEntry multiUIDtemplate = new ExternalEntry(uid);
     * Entry[] result = space.readMultiple(multiUIDtemplate , null , Integer.MAX_VALUE);
     * for (int i=0; i < result.length ;i++ )
     * {
     * ExternalEntry ee = (ExternalEntry)result[i];
     * System.out.println(ee.getEntry(space));
     * }
     *
     * </code>
     * </pre>
     *
     * @param multipleUIDs Entries UIDs array.
     */
    public ExternalEntry(String[] multipleUIDs)    // operate on multiple entries by uids
    {
        m_MultipleUIDs = multipleUIDs;
    }

    /**
     * Constructs an ExternalEntry object. Use this constructor to introduce a new class to the
     * space.
     *
     * @param className    The entry class name
     * @param fieldsValues The entry field values array
     * @param fieldsNames  The entry field names array
     * @param fieldsTypes  The entry field types array. Field types should be Java full class
     *                     names.
     */
    public ExternalEntry(String className, Object[] fieldsValues, String[] fieldsNames, String[] fieldsTypes) {
        this(className, fieldsValues, fieldsNames);
        this.m_FieldsTypes = fieldsTypes;
    }

    /**
     * Constructs an ExternalEntry object. Use this constructor to introduce a new class to the
     * space.
     *
     * @param className    The Entry class name
     * @param fieldsValues The Entry field values array
     * @param fieldsNames  The Entry field names array
     */
    public ExternalEntry(String className, Object[] fieldsValues, String[] fieldsNames) {
        if (className == null) {
            throw new RuntimeException("ExternalEntry problem, Class Name is NULL !");
        }
        m_ClassName = className;
        int values_size = fieldsValues == null ? 0 : fieldsValues.length;
        int names_size = fieldsNames == null ? 0 : fieldsNames.length;
        if (values_size != 0 && names_size != 0 && values_size != names_size) {
            throw new RuntimeException("ExternalEntry problem, values do not match fields names !");
        }
        m_FieldsValues = fieldsValues;
        m_FieldsNames = fieldsNames;
    }

    /**
     * Constructs an ExternalEntry object. Do not use this constructor to introduce new the class to
     * the space.
     *
     * @param className    The class name
     * @param fieldsValues Entry field values array
     */
    public ExternalEntry(String className, Object[] fieldsValues) {
        this(className, fieldsValues, (String[]) null);
    }

    public boolean[] getPrimitiveFields() {
        return _primitiveFields;
    }

    public boolean hasExtendedInfo() {
        return _returnTrueType || m_RangeValues != null || m_ExtendedMatchCodes != null || m_RangeValuesInclusion != null;
    }

    private void writePrimitiveObject(ObjectOutput out, Object obj, int indexField) {
        try {
            IOUtils.writeObject(out, obj);
        } catch (IOException ex) {
            String fieldName = "";
            String fieldValue = "";

            /** print more information if we have full packet, if <code>null</code> we have thin packet */
            if (m_FieldsNames != null && m_FieldsNames[indexField] != null)
                fieldName = "\nField Name      : " + m_FieldsNames[indexField];

            if (m_FieldsValues != null && m_FieldsValues[indexField] != null)
                fieldValue = "\nField Type     : " + m_FieldsValues[indexField].getClass().getName();

            throw new EntrySerializationException("Failed to serialize Entry field." +
                    "\nEntry Class name : " + m_ClassName +
                    fieldValue +
                    fieldName, ex);
        }
    }

    /**
     * Reads from input stream the primitive type according to the bit code (which indicates the
     * primitive type), then it uses the right read method to fetch the Primitive type itself.
     *
     * This way we avoid using readObject() in case of primitive types, which is very expensive.
     *
     * @return the primitive data
     */
    private Object readPrimitiveObject(ObjectInput in, int indexField)
            throws IOException, ClassNotFoundException {
        try {
            return IOUtils.readObject(in);
        } catch (IOException ex) {
            String fieldName = "";
            String fieldValue = "";

            /** print more information if we have full packet, if <code>null</code> we have thin packet */
            if (m_FieldsNames != null && m_FieldsNames[indexField] != null)
                fieldName = "\nField Name      : " + m_FieldsNames[indexField];

            if (m_FieldsValues != null && m_FieldsValues[indexField] != null)
                fieldValue = "\nField Type     : " + m_FieldsValues[indexField].getClass().getName();

            throw new EntrySerializationException("Failed to deserialize Entry field." +
                    "\nEntry Classname : " + m_ClassName +
                    fieldValue +
                    fieldName, ex);
        }
    }

    private final static class BitMap {
        private static final int UID = 0x10000000;
        private static final int CLASSNAME = 0x20000000;
        private static final int SUPER_CLASSES = 0x40000000;
        private static final int FIELDS_NAMES = 0x80000000;
        private static final int FIELDS_TYPES = 0x01000000;
        private static final int FIELDS_VALUES = 0x02000000;
        private static final int INDEX_INDICATORS = 0x04000000;
        private static final int REPLICATABLE = 0x08000000;
        private static final int MULTIPLE_UIDS = 0x00100000;
        private static final int RETURN_ONLY_UIDS = 0x00200000;
        private static final int IS_FIFO = 0x00400000;
        private static final int EXTENDED_MATCH = 0x00800000;
        private static final int RANGE_VALUES = 0x00010000;
        private static final int TIME_TO_LIVE = 0x00020000;
        private static final int IS_TRANSIENT = 0x00040000;
        private static final int NO_WRITE_LEASE = 0x00080000;
        private static final int RANGE_INCLUSION = 0x00001000;
    }

    private int buildFlags() {
        int flags = 0;

        if (m_UID != null) {
            flags |= BitMap.UID;
        }
        if (m_ClassName != null) {
            flags |= BitMap.CLASSNAME;
        }
        if (m_SuperClassesNames != null) {
            flags |= BitMap.SUPER_CLASSES;
        }
        if (m_FieldsNames != null) {
            flags |= BitMap.FIELDS_NAMES;
        }
        if (m_FieldsTypes != null) {
            flags |= BitMap.FIELDS_TYPES;
        }
        if (m_FieldsValues != null) {
            flags |= BitMap.FIELDS_VALUES;
        }
        if (m_IndexIndicators != null) {
            flags |= BitMap.INDEX_INDICATORS;
        }
        if (m_Replicatable) {
            flags |= BitMap.REPLICATABLE;
        }
        if (m_MultipleUIDs != null) {
            flags |= BitMap.MULTIPLE_UIDS;
        }
        if (m_ReturnOnlyUids) {
            flags |= BitMap.RETURN_ONLY_UIDS;
        }
        if (m_isFifo) {
            flags |= BitMap.IS_FIFO;
        }
        if (m_ExtendedMatchCodes != null) {
            flags |= BitMap.EXTENDED_MATCH;
        }
        if (m_RangeValues != null) {
            flags |= BitMap.RANGE_VALUES;
        }

        if (m_RangeValuesInclusion != null) {
            flags |= BitMap.RANGE_INCLUSION;
        }

        if (m_TimeToLive != 0) {
            flags |= BitMap.TIME_TO_LIVE;
        }
        if (m_isTransient) {
            flags |= BitMap.IS_TRANSIENT;
        }
        if (m_NOWriteLeaseMode) {
            flags |= BitMap.NO_WRITE_LEASE;
        }

        return flags;
    }

    /**
     * Save the object contents by calling the methods of DataOutput for its primitive values or
     * calling the writeObject method of ObjectOutput for objects, strings, and arrays.
     *
     * @param out the stream to write the object to
     * @throws IOException Includes any I/O exceptions that may occur
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            out.writeInt(buildFlags());

            if (m_UID != null) {
                out.writeUTF(m_UID);
                out.writeInt(m_VersionID);
                if (m_TimeToLive != 0)
                    out.writeLong(m_TimeToLive);
            }

            if (m_ClassName != null) {
                out.writeUTF(m_ClassName);
            }

            // field values
            if (m_FieldsValues != null) {
                // write the value array
                int size = m_FieldsValues.length;
                out.writeInt(size);
                int numNonNullFields = 0;  // # of non-null fields(used for serialization)
                boolean compressedMode = false;
                if (size > 3) {
                    for (int i = 0; i < size; i++) {
                        if (m_FieldsValues[i] != null)
                            numNonNullFields++;
                    }
                    if (numNonNullFields < size / 2)
                        compressedMode = true;
                    out.writeBoolean(compressedMode);
                    if (compressedMode)
                        out.writeInt(numNonNullFields);
                }

                for (int i = 0; i < size; i++) {
                    if (m_FieldsValues[i] != null) {
                        if (compressedMode)
                            out.writeInt(i); // value position
                        else
                            out.writeBoolean(true); //value present

                        //check if primitive type, if it is, we call
                        //the right write method and not the writeObject()
                        writePrimitiveObject(out, m_FieldsValues[i], i);
                    } else // null value
                    {
                        if (!compressedMode)
                            out.writeBoolean(false); //value present
                    }
                }
            }

            if (m_FieldsNames != null) {
                out.writeInt(m_FieldsNames.length);
                for (int i = 0; i < m_FieldsNames.length; i++)
                    out.writeUTF(m_FieldsNames[i]);
            }

            if (m_FieldsTypes != null) {
                out.writeInt(m_FieldsTypes.length);
                for (int i = 0; i < m_FieldsTypes.length; i++)
                    out.writeUTF(m_FieldsTypes[i]);
            }

            if (m_IndexIndicators != null) {
                out.writeInt(m_IndexIndicators.length);
                for (int i = 0; i < m_IndexIndicators.length; i++)
                    out.writeBoolean(m_IndexIndicators[i]);
            }

            //entity format
            out.writeShort(0 /*_objectFormat*/);

            if (m_SuperClassesNames != null) {
                out.writeInt(m_SuperClassesNames.length);
                for (int i = 0; i < m_SuperClassesNames.length; i++) {
                    out.writeUTF(m_SuperClassesNames[i]);
                }
            }

            if (m_MultipleUIDs != null) {
                out.writeInt(m_MultipleUIDs.length);
                for (int i = 0; i < m_MultipleUIDs.length; i++) {
                    out.writeUTF(m_MultipleUIDs[i]);
                }
            }

            if (m_ExtendedMatchCodes != null) {
                out.writeInt(m_ExtendedMatchCodes.length);
                for (int i = 0; i < m_ExtendedMatchCodes.length; i++) {
                    out.writeShort(m_ExtendedMatchCodes[i]);
                }
            }

            if (m_RangeValues != null) {
                out.writeInt(m_RangeValues.length);
                for (int i = 0; i < m_RangeValues.length; i++) {
                    out.writeObject(m_RangeValues[i]);
                }
            }

            if (m_RangeValuesInclusion != null) {
                out.writeInt(m_RangeValuesInclusion.length);
                for (int i = 0; i < m_RangeValuesInclusion.length; i++) {
                    out.writeObject(m_RangeValuesInclusion[i]);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof EntrySerializationException)
                throw (EntrySerializationException) ex;

            String className = m_ClassName != null ? m_ClassName : ".";
            throw new EntrySerializationException("Failed to serialize Entry " + className, ex);
        }
    }


    /**
     * Restore the class contents by calling the methods of DataInput for primitive types and
     * readObject for objects, strings and arrays.
     *
     * @param in the stream to read data from in order to restore the object
     * @throws IOException            if I/O errors occur
     * @throws ClassNotFoundException If the class for an object being restored cannot be found.
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        try {
            int flags = in.readInt();

            if ((flags & BitMap.UID) != 0) // UID not a null
            {
                m_UID = in.readUTF();
                m_VersionID = in.readInt();
                if ((flags & BitMap.TIME_TO_LIVE) != 0) //TimeToLive not zero
                    m_TimeToLive = in.readLong();
            }

            if ((flags & BitMap.CLASSNAME) != 0) // classname not a null class
            {
                m_ClassName = in.readUTF();
            }

            m_Replicatable = (flags & BitMap.REPLICATABLE) != 0;
            m_ReturnOnlyUids = (flags & BitMap.RETURN_ONLY_UIDS) != 0;
            m_isFifo = (flags & BitMap.IS_FIFO) != 0;
            m_isTransient = (flags & BitMap.IS_TRANSIENT) != 0;
            m_NOWriteLeaseMode = (flags & BitMap.NO_WRITE_LEASE) != 0;

            if ((flags & BitMap.FIELDS_VALUES) != 0) // field-values
            {
                //  value array size
                int size = in.readInt();
                m_FieldsValues = new Object[size];

                int numNonNullFields = 0;  // # of non-null fields
                boolean compressedMode = false;

                if (size > 3) {
                    compressedMode = in.readBoolean();
                    if (compressedMode)
                        numNonNullFields = in.readInt();
                }

                if (compressedMode) {
                    for (int i = 0; i < numNonNullFields; i++) {
                        int indx = in.readInt();
                        m_FieldsValues[indx] = readPrimitiveObject(in, indx);
                    }
                } //compressed
                else { //!compressed
                    for (int i = 0; i < size; i++) {
                        if (in.readBoolean())
                            m_FieldsValues[i] = readPrimitiveObject(in, i);
                    }
                }
            } //field-values

            //METADATA SECTION
            if ((flags & BitMap.FIELDS_NAMES) != 0) {
                int size = in.readInt();
                m_FieldsNames = new String[size];
                for (int i = 0; i < size; i++) {
                    m_FieldsNames[i] = in.readUTF();
                }
            }

            if ((flags & BitMap.FIELDS_TYPES) != 0) {
                int size = in.readInt();
                m_FieldsTypes = new String[size];
                for (int i = 0; i < size; i++) {
                    m_FieldsTypes[i] = in.readUTF();
                }
            }

            if ((flags & BitMap.INDEX_INDICATORS) != 0) {
                int size = in.readInt();
                m_IndexIndicators = new boolean[size];
                for (int i = 0; i < size; i++) {
                    m_IndexIndicators[i] = in.readBoolean();
                }
            }

            //entity format
            int objectFormat = in.readShort();

            // m_SuperClassesNames
            if ((flags & BitMap.SUPER_CLASSES) != 0) {
                int size = in.readInt();
                m_SuperClassesNames = new String[size];
                for (int i = 0; i < size; i++)
                    m_SuperClassesNames[i] = in.readUTF();
            }

            if ((flags & BitMap.MULTIPLE_UIDS) != 0) {
                int size = in.readInt();
                m_MultipleUIDs = new String[size];
                for (int i = 0; i < size; i++)
                    m_MultipleUIDs[i] = in.readUTF();
            }

            if ((flags & BitMap.EXTENDED_MATCH) != 0) {
                int size = in.readInt();
                m_ExtendedMatchCodes = new short[size];
                for (int i = 0; i < size; i++)
                    m_ExtendedMatchCodes[i] = in.readShort();
            }

            if ((flags & BitMap.RANGE_VALUES) != 0) {
                int size = in.readInt();
                m_RangeValues = new Object[size];
                for (int i = 0; i < size; i++)
                    m_RangeValues[i] = in.readObject();
            }

            if ((flags & BitMap.RANGE_INCLUSION) != 0) {
                int size = in.readInt();
                m_RangeValuesInclusion = new boolean[size];
                for (int i = 0; i < size; i++)
                    m_RangeValuesInclusion[i] = in.readBoolean();
            }
        } catch (Exception ex) {
            if (ex instanceof EntrySerializationException)
                throw (EntrySerializationException) ex;

            String className = m_ClassName != null ? m_ClassName : ".";
            throw new EntrySerializationException("Failed to deserialize Entry " + className, ex);
        }
    }


    /**
     * Checks for identical UIDs and field values.
     *
     * @param obj the <code>ExternalEntry</code> to compare
     * @return Returns true if identical UID and field values.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)    //check ref. first
            return true;

        if (!(obj instanceof ExternalEntry))
            return false;

        ExternalEntry e = (ExternalEntry) obj;

        //match uids && fields
        return equals(m_UID, e.m_UID) && equals(m_FieldsValues, e.m_FieldsValues);
    }

    /**
     * General equals method that compares two objects considering null values and multi-dimensional
     * arrays. In case of arrays - their members are compared recursively.
     *
     * @return true if two objects are equal
     */
    public static boolean equals(Object o1, Object o2) {
        // First check references
        if (o1 == o2)
            return true;

        // Check if one of the objects is null
        if (o1 == null || o2 == null)
            return false;

        // Check that the objects are of the same type
        Class c = o1.getClass();
        if (c != o2.getClass())
            return false;

        // Check for native array types
        // If array - compare all the members
        if (c.isArray()) {
            int length = Array.getLength(o1);
            int length2 = Array.getLength(o2);

            // Check arrays length
            if (length != length2)
                return false;

            // Check arrays members
            for (int i = 0; i < length; i++) {
                Object c1 = Array.get(o1, i);
                Object c2 = Array.get(o2, i);

                if (!equals(c1, c2))
                    return false;
            }

            return true;
        } else {
            return o1.equals(o2);
        }


    }

    /**
     * Entry UID hashCode.
     *
     * @return Returns the Entry UID hashCode.
     */
    @Override
    public int hashCode() {
        return (m_UID != null) ? m_UID.hashCode() : 10;
    }

    /**
     * {@inheritDoc}
     */
    public String getClassName() {
        return m_ClassName;
    }

    /**
     * This method should be used for new Entry classes introduced to the space.
     *
     * @param className The Entry class Name to set.
     */
    public void setClassName(String className) {
        m_ClassName = className;
    }

    /**
     * Matching codes array.
     *
     * @return Returns the Matching codes array.
     */
    public short[] getExtendedMatchCodes() {
        return m_ExtendedMatchCodes;
    }

    /**
     * The matching codes.
     *
     * @param extendedMatchCodes The matching codes to set for template objects.
     *                           <pre>
     *                                                     This array should match the field values
     *                           array.
     *                                                     Optional Values:
     *                                                     TemplateMatchCodes.EQ - equal
     *                                                     TemplateMatchCodes.NE - not equal
     *                                                     TemplateMatchCodes.GT - greater than
     *                                                     TemplateMatchCodes.GE - grater-equal
     *                                                     TemplateMatchCodes.LT - less than
     *                                                     TemplateMatchCodes.LE - less-equal
     *                                                     TemplateMatchCodes.IS_NULL - entry field
     *                           is null (template field
     *                                                     not relevant)
     *                                                     TemplateMatchCodes.NOT_NULL - entry field
     *                           is not null (template
     *                                                     field not relevant)
     *                                                     </pre>
     */
    public void setExtendedMatchCodes(short[] extendedMatchCodes) {
        m_ExtendedMatchCodes = extendedMatchCodes;
    }

    /**
     * {@inheritDoc}
     */
    public String[] getFieldsNames() {
        return m_FieldsNames;
    }

    /**
     * Set the entry field names.
     *
     * @param fieldsNames The entry field names
     */
    public void setFieldsNames(String[] fieldsNames) {
        m_FieldsNames = fieldsNames;
    }

    /**
     * {@inheritDoc}
     */
    public String[] getFieldsTypes() {
        return m_FieldsTypes;
    }

    /**
     * Entry fields types to set.
     *
     * @param fieldsTypes The Entry fields types to set. Field types are Java classes full names.
     */
    public void setFieldsTypes(String[] fieldsTypes) {
        m_FieldsTypes = fieldsTypes;
    }

    /**
     * {@inheritDoc}
     */
    public Object[] getFieldsValues() {
        return m_FieldsValues;
    }

    /**
     * Entry field values to set.
     *
     * @param fieldsValues Entry field values to set
     */
    public void setFieldsValues(Object[] fieldsValues) {
        m_FieldsValues = fieldsValues;
    }

    /**
     * Entry range Values.
     *
     * @return Returns the entry range Values
     */
    public Object[] getRangeValues() {
        return m_RangeValues;
    }

    /**
     * Entry range values to set.
     *
     * @param rangeValues Entry range values to set
     */
    public void setRangeValues(Object[] rangeValues) {
        m_RangeValues = rangeValues;
    }

    /**
     * FIFO Indication.
     *
     * @return Returns the FIFO Indication.
     */
    public boolean isFifo() {
        return m_isFifo;
    }

    /**
     * Set the FIFO indication.
     *
     * @param fifo FIFO indication. This method should be used for new Entry Classes introduced to
     *             the space and for templates objects used for matching.
     */
    public void setFifo(boolean fifo) {
        m_isFifo = fifo;
    }

    /**
     * {@inheritDoc}
     */
    public boolean[] getIndexIndicators() {
        return m_IndexIndicators;
    }

    /**
     * indexed fields array indication to set.
     *
     * @param indexIndicators Array of fields to Index. The values of this array should match the
     *                        fields array
     */
    public void setIndexIndicators(boolean[] indexIndicators) {
        m_IndexIndicators = indexIndicators;
    }


    /**
     * The field name representing the primary key. Usually the field name of the first indexed
     * field (can be null).
     *
     * @return field name of the primary key.
     * @see #setPrimaryKeyName(String)
     */
    public String getPrimaryKeyName() {
        return m_PrimaryKeyName;
    }

    /**
     * Sets the field name representing the primary key. Usually the field name of the first indexed
     * field.
     *
     * @param fieldName field name of the primary key.
     * @see #getPrimaryKeyName()
     */
    public void setPrimaryKeyName(String fieldName) {
        m_PrimaryKeyName = fieldName;
    }

    /**
     * Array of Entry UIDs.
     *
     * @return Returns Array of Entry UIDs
     */
    public String[] getMultipleUIDs() {
        return m_MultipleUIDs;
    }

    /**
     * Array of Entry UIDs to read.
     *
     * @param multipleUIDs Array of Entry UIDs to read.
     */
    public void setMultipleUIDs(String[] multipleUIDs) {
        m_MultipleUIDs = multipleUIDs;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isReplicatable() {
        return m_Replicatable;
    }

    /**
     * Set the replicatable indication value.
     *
     * @param replicatable The replicatable indication value. This method should be used with
     *                     clustered spaces.
     */
    public void setReplicatable(boolean replicatable) {
        m_Replicatable = replicatable;
    }

    /**
     * ReturnOnlyUids indication.
     *
     * @return Returns the ReturnOnlyUids indication
     */
    public boolean isReturnOnlyUids() {
        return m_ReturnOnlyUids;
    }

    /**
     * set the ReturnOnlyUids indication.
     *
     * @param returnOnlyUids set the returnOnlyUids indication value
     */
    public void setReturnOnlyUids(boolean returnOnlyUids) {
        m_ReturnOnlyUids = returnOnlyUids;
    }

    /**
     * {@inheritDoc}
     */
    public String[] getSuperClassesNames() {
        return m_SuperClassesNames;
    }

    /**
     * {@inheritDoc}
     *
     * @return default implementation returns null.
     */
    public String getCodebase() {
        return null;
    }

    /**
     * set the Super Classes Names Array.
     *
     * @param superClassesNames The Super Classes Names Array
     */
    public void setSuperClassesNames(String[] superClassesNames) {
        m_SuperClassesNames = superClassesNames;
    }

    /**
     * {@inheritDoc}
     */
    public long getTimeToLive() {
        return m_TimeToLive;
    }

    /**
     * Set the time left for this entry to live.
     *
     * @param timeToLive in milliseconds
     */
    public ExternalEntry setTimeToLive(long timeToLive) {
        m_TimeToLive = timeToLive;
        return this;
    }

    /**
     * {@inheritDoc} Inherited from {@link IGSEntry} and is equivalent to calling {@link
     * #getVersionID()}
     *
     * @see #getVersionID()
     * @see #setVersionID(int)
     */
    public int getVersion() {
        return m_VersionID;
    }

    public void setVersion(int version) {
        m_VersionID = version;
    }

    /**
     * Check if Transient entry.
     *
     * @return Returns true if Transient Entry.
     */
    public boolean isTransient() {
        return m_isTransient;
    }

    /**
     * Sets the entry to be transient (true) or persistent (false).
     */
    public void setTransient(boolean isTransient) {
        m_isTransient = isTransient;
    }

    /**
     * Construct Transient Entry. This method should be used with Persistent spaces. This method
     * should be used with new entries.
     */
    public void makeTransient() {
        m_isTransient = true;
    }

    /**
     * Construct Persistent Entry. This method should be used with Persistent spaces. This method
     * should be used with new entries.
     */
    public void makePersistent() {
        m_isTransient = false;
    }

    /**
     * {@inheritDoc}
     */
    public Map.Entry getMapEntry() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public String getUID() {
        return m_UID;
    }

    /**
     * Set Entry UID.
     *
     * @param m_uid The Entry UID to set. When using this method make sure you are using unique
     *              value. This method should get as input returned value from
     *              ClientUIDHandler.createUIDFromName
     *              <pre>
     *                           The UID is a String based identifier and composed of the following
     *              parts:
     *                           - Class information  class Hashcode and name size
     *                           - Space node name At clustered environment combined from
     *              container-name :space
     *                           name. At non-clustered environment combined from dummy name.
     *                           - Timestamp
     *                           - Counter
     *                           </pre>
     */
    public void setUID(String m_uid) {
        m_UID = m_uid;
    }

    /**
     * Entry Version ID.
     *
     * @return Returns the Entry Version ID. The version number is incremented each time an entry is
     * updated. The initial value to be stored in the space is 1, and is incremented after each
     * update. This value is used when running in optimistic locking mode.
     */
    public int getVersionID() {
        return getVersion();
    }

    /**
     * Entry Version ID.
     *
     * @param versionID The Entry Version ID to set.
     */
    public void setVersionID(int versionID) {
        setVersion(versionID);
    }


    /**
     * Set <code>true</code> do not return Lease object after write, <code>false</code> return Lease
     * object after write.
     *
     * @param noWriteLeaseMode write mode.
     **/
    public void setNOWriteLeaseMode(boolean noWriteLeaseMode) {
        m_NOWriteLeaseMode = noWriteLeaseMode;
    }

    /**
     * Check write mode.
     *
     * @return <code>true</code> if do not return Lease object after write, otherwise
     * <code>false</code>.
     **/
    public boolean isNOWriteLeaseMode() {
        return m_NOWriteLeaseMode;
    }

    /**
     * {@inheritDoc}
     */
    public Object getFieldValue(String fieldName) {
        return getFieldValue(getFieldPosition(fieldName));
    }

    /**
     * {@inheritDoc}
     */
    public Object setFieldValue(String fieldName, Object value) {
        return setFieldValue(getFieldPosition(fieldName), value);
    }

    /**
     * {@inheritDoc}
     */
    public String getFieldType(String fieldName) {
        int index = getFieldPosition(fieldName);
        try {
            return m_FieldsTypes[index];
        } catch (Exception e) {
            throw new IllegalStateException("The field types array was not properly set", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean isIndexedField(String fieldName) {
        int index = getFieldPosition(fieldName);
        try {
            return m_IndexIndicators[index];
        } catch (Exception e) {
            throw new IllegalStateException("The field indexes array was not properly set", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object getFieldValue(int index) throws IllegalArgumentException, IllegalStateException {
        try {
            return m_FieldsValues[index];
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object setFieldValue(int index, Object value) throws IllegalArgumentException, IllegalStateException {
        try {
            Object oldValue = m_FieldsValues[index];
            m_FieldsValues[index] = value;
            return oldValue;
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public int getFieldPosition(String fieldName) {
        if (m_FieldsNames != null) {
            for (int i = 0; i < m_FieldsNames.length; ++i) {
                if (m_FieldsNames[i].equals(fieldName))
                    return i;
            }
        }
        throw new IllegalArgumentException("Field name " + fieldName + " is not available in class " + getClassName());
    }

    /**
     *
     */
    public Entry getEntry(IJSpace space) throws UnusableEntryException {
        return (Entry) getObject(space);
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(IJSpace space) throws UnusableEntryException {
        return ((ISpaceProxy) space).getDirectProxy().getTypeManager().getObjectFromIGSEntry(this);
    }

    /**
     * Get the routing field name that was selected to be the first index for partition.
     *
     * @return the routing field name
     */
    public String getRoutingFieldName() {
        return routingFieldName;
    }

    /**
     * Set the routing field name that was selected to be the first index for partition.
     *
     * @param routingFieldName the routing field name
     */
    public void setRoutingFieldName(String routingFieldName) {
        this.routingFieldName = routingFieldName;
    }

    /**
     * Shallow clone
     *
     * @return a shallow copy of this instance
     */
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) // can never happen.
        {
        }
        return null;
    }


    /**
     * Get extended match code at specified index
     *
     * @return the match code for the given property index
     */
    public short getExtendedMatchCode(int index) {
        if (m_ExtendedMatchCodes == null)
            return 0;

        return m_ExtendedMatchCodes[index];
    }

    /**
     * Get range value  at specified index
     *
     * @return the range value for the given property index
     */
    public Object getRangeValue(int index) {
        if (m_RangeValues == null)
            return null;

        return m_RangeValues[index];
    }

    /**
     * Get range value inclusion indicator at specified index
     *
     * @return the inclusion indicator for the given property index
     */
    public boolean getRangeValueInclusion(int index) {
        if (m_RangeValuesInclusion == null)
            return true;

        return m_RangeValuesInclusion[index];
    }

    /**
     * @return true if external entry has extended matching
     */
    public boolean isExtended() {
        return m_ClassName != null && m_ExtendedMatchCodes != null && m_FieldsNames != null && m_FieldsNames.length > 0;
    }

    /**
     * Returns a string representation of the {@link ExternalEntry}.
     *
     * @return a string representation of the {@link ExternalEntry}.
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Class Name: ").append(getClassName());
        str.append("\nSuper Classes: ").append(Arrays.toString(getSuperClassesNames()));
        str.append("\nUID: ").append(getUID());
        str.append("\nFields Values: ").append(Arrays.toString(getFieldsValues()));
        str.append("\nFields Names: ").append(Arrays.toString(getFieldsNames()));
        str.append("\nFields Types: ").append(Arrays.toString(getFieldsTypes()));
        return str.toString();
    }
}