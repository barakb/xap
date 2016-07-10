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

package com.j_spaces.jms.utils;

import java.io.ByteArrayOutputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * A collection of String utilities. This class provides a set of static functions for building
 * string representations of complex structures and other String validating utils.
 *
 * @author Gershon Diner
 * @version 4.0
 **/
public class StringsUtils {

    /**
     * An empty string constant
     */
    public static final String EMPTY = "";

    public StringsUtils() {
    }

    /**
     * List of valid Java keywords, see The Java Language Specification Second Edition Section 3.9,
     * 3.10.3 and 3.10.7
     */
    private static final String[] keywords =
            {
                    "abstract",
                    "boolean",
                    "break",
                    "byte",
                    "case",
                    "catch",
                    "char",
                    "class",
                    "const",
                    "continue",
                    "default",
                    "do",
                    "double",
                    "else",
                    "extends",
                    "final",
                    "finally",
                    "float",
                    "for",
                    "goto",
                    "if",
                    "implements",
                    "import",
                    "instanceof",
                    "int",
                    "interface",
                    "long",
                    "native",
                    "new",
                    "package",
                    "private",
                    "protected",
                    "public",
                    "return",
                    "short",
                    "static",
                    "strictfp",
                    "super",
                    "switch",
                    "synchronized",
                    "this",
                    "throw",
                    "throws",
                    "transient",
                    "try",
                    "void",
                    "volatile",
                    "while",

                    "true",     // technically no keywords but we are not picky
                    "false",
                    "null"
            };

    /**
     * List of EJB-QL Identifiers as defined in the EJB 2.0 Specification Section 11.2.6.1
     */
    private static final String[] ejbQlIdentifiers =
            {
                    "AND",
                    "AS",
                    "BETWEEN",
                    "DISTINCT",
                    "EMPTY",
                    "FALSE",
                    "FROM",
                    "IN",
                    "IS",
                    "LIKE",
                    "MEMBER",
                    "NOT",
                    "NULL",
                    "OBJECT",
                    "OF",
                    "OR",
                    "SELECT",
                    "UNKNOWN",
                    "TRUE",
                    "WHERE",
            };

    /**
     * Check whether the given String is an identifier according to the EJB-QL definition. See The
     * EJB 2.0 Documentation Section 11.2.6.1.
     *
     * @param s String to check
     * @return <code>true</code> if the given String is a reserved identifier in EJB-QL,
     * <code>false</code> otherwise.
     */
    public final static boolean isEjbQlIdentifier(String s) {
        if (s == null || s.length() == 0) {
            return false;
        }

        for (int i = 0; i < ejbQlIdentifiers.length; i++) {
            if (ejbQlIdentifiers[i].equalsIgnoreCase(s)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if the given string is empty or null
     *
     * @param string String to check
     * @return True if string is empty or null
     */
    public static boolean isEmpty(String string) {
        if (null == string)
            return true;
        return string.equals(EMPTY);
    }

    /**
     * Check whether the given String is a valid identifier according to the Java Language
     * specifications.
     *
     * See The Java Language Specification Second Edition, Section 3.8 for the definition of what is
     * a valid identifier.
     *
     * @param s String to check
     * @return <code>true</code> if the given String is a valid Java identifier, <code>false</code>
     * otherwise.
     */
    public final static boolean isValidJavaIdentifier(String s) {
        // an empty or null string cannot be a valid identifier
        if (s == null || s.length() == 0) {
            return false;
        }

        char[] c = s.toCharArray();
        if (!Character.isJavaIdentifierStart(c[0])) {
            return false;
        }

        for (int i = 1; i < c.length; i++) {
            if (!Character.isJavaIdentifierPart(c[i])) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check whether the given String is a reserved Java Keyword according to the Java Language
     * Specifications.
     *
     * @param s String to check
     * @return <code>true</code> if the given String is a reserved Java keyword, <code>false</code>
     * otherwise.
     */
    public final static boolean isJavaKeyword(String s) {
        if (s == null || s.length() == 0) {
            return false;
        }

        for (int i = 0; i < keywords.length; i++) {
            if (keywords[i].equals(s)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Provides a string representation of an object. Checks if there exists in this class a
     * specialized <code>toString</code> function for the object class, or calls the
     * <code>toString</code> function of the object.
     *
     * @param output a buffer to print the object into
     * @param obj    the object to print
     */
    public static final void toString(StringBuffer output, Object obj) {
        if (obj == null) {
            output.append("null");
            return;
        }

        if (obj instanceof String) {
            toString(output, (String) obj);
            return;
        }
        if (obj instanceof Vector) {
            toString(output, (Vector) obj);
            return;
        }
        if (obj instanceof Hashtable) {
            toString(output, (Hashtable) obj);
            return;
        }

        Class type = obj.getClass();
        if (type.isArray()) {
            toString(output, obj, type.getComponentType());
            return;
        }

        output.append(obj.toString());
    }

    /**
     * Provides a string representation of an object. Calls <code>toString(StringBuffer)</code>.
     *
     * @param obj the object to print
     * @return a string representation of the object
     */
    public static final String toString(Object obj) {
        if (obj == null)
            return "null";

        if (obj instanceof String) {
            // this avoids creating a useless StringBuffer
            return toString((String) obj);
        }

        StringBuffer output = new StringBuffer();
        toString(output, obj);
        return output.toString();
    }

    /**
     * Provides a Java string literal representing the parameter string. This includes surrounding
     * double quotes, and quoted special characters, including UTF escape sequences when necessary.
     * <p> This function works only for ASCII character encoding, and assumes this is the default
     * encoding.
     *
     * @param output a byte buffer to print the object into
     * @param str    the string to print
     */
    public static final void toByteArray(ByteArrayOutputStream output,
                                         String str) {
        if (str == null) {
            return;
        }

        output.write(34);    // '"'

        int max = str.length();
        for (int i = 0; i < max; i++) {
            // gets the numeric value of the unicode character
            int b = str.charAt(i);
            if ((b >= 32) && (b <= 126)) {
                // ASCII printable character, includes '"' and '\\'
                switch (b) {
                    case 34:    // '"'
                    case 92:    // '\\'
                        output.write(92);    // '\\'
                        break;
                }
                output.write(b);
            } else {
                output.write(92);    // '\\'
                switch (b) {
                    case 8:        // backspace
                        output.write(98);    // 'b'
                        break;
                    case 9:        // horizontal tab
                        output.write(116);    // 't'
                        break;
                    case 10:    // linefeed
                        output.write(110);    // 'n'
                        break;
                    case 12:    // form feed
                        output.write(102);    // 'f'
                        break;
                    case 13:    // carriage return
                        output.write(114);    // 'r'
                        break;
                    default:
                        // UTF escape sequence
                        output.write(117);    // 'u'
                        int b3 = b >> 4;
                        int b4 = b & 0xf;
                        if (b4 < 10)
                            b4 += 48;        // '0'
                        else
                            b4 += 87;        // 'a' - 10
                        int b2 = b3 >> 4;
                        b3 &= 0xf;
                        if (b3 < 10)
                            b3 += 48;        // '0'
                        else
                            b3 += 87;        // 'a' - 10
                        int b1 = b2 >> 4;
                        b2 &= 0xf;
                        if (b2 < 10)
                            b2 += 48;        // '0'
                        else
                            b2 += 87;        // 'a' - 10
                        if (b1 < 10)
                            b1 += 48;        // '0'
                        else
                            b1 += 87;        // 'a' - 10
                        output.write(b1);
                        output.write(b2);
                        output.write(b3);
                        output.write(b4);
                        break;
                }
            }
        }

        output.write(34);    // '"'
    }

    /**
     * Provides a Java string literal representing the parameter string. This includes surrounding
     * double quotes, and quoted special characters, including UTF escape sequences when necessary.
     * <p> This function works only for ASCII character encoding, and assumes this is the default
     * encoding.
     *
     * @param output a string buffer to print the object into
     * @param str    the string to print
     */
    public static final void toString(StringBuffer output, String str) {
        if (str == null) {
            output.append("null");
            return;
        }

        output.append(toString(str));
    }

    /**
     * Provides a Java string literal representing the parameter string. This includes surrounding
     * double quotes, and quoted special characters, including UTF escape sequences when necessary.
     * <p> This function works only for ASCII character encoding, and assumes this is the default
     * encoding.
     *
     * @param str the string to print
     * @return a Java string literal representation of the string
     */
    public static final String toString(String str) {
        if (str == null)
            return "null";

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        toByteArray(buffer, str);
        return buffer.toString();
    }

    /**
     * Controls the formatting of lists of objects. Lists with a number of elements up to
     * <code>listMax</code> are entirely printed. A value of <code>-1</code> leads to complete
     * printing of the list, whatever its size. <p> This variable, when used in an agent server, may
     * be set by the debug variable <code>Debug.var.fr.dyade.aaa.util.listMax</code>. Its default
     * value is <code>10</code>.
     */
    final public static int listMax = 10;

    /**
     * Controls the formatting of lists of objects. Lists with a number of elements greater than
     * <code>listMax</code> are partially printed, with the <code>listBorder</code> leading and
     * trailing elements. <p> This variable, when used in an agent server, may be set by the debug
     * variable <code>Debug.var.fr.dyade.aaa.util.listBorder</code>. Its default value is
     * <code>3</code>.
     */
    final public static int listBorder = 3;

    /**
     * Provides a string representation of an array. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param obj    the array to print
     * @param type   the type of the array components
     */
    public static final void toString(StringBuffer output,
                                      Object obj, Class type) {
        if (obj == null) {
            output.append("null");
            return;
        }

        if (type.isPrimitive()) {
            if (type == Boolean.TYPE)
                toString(output, (boolean[]) obj);
            else if (type == Character.TYPE)
                toString(output, (char[]) obj);
            else if (type == Byte.TYPE)
                toString(output, (byte[]) obj);
            else if (type == Short.TYPE)
                toString(output, (short[]) obj);
            else if (type == Integer.TYPE)
                toString(output, (int[]) obj);
            else if (type == Long.TYPE)
                toString(output, (long[]) obj);
            else if (type == Float.TYPE)
                toString(output, (float[]) obj);
            else if (type == Double.TYPE)
                toString(output, (double[]) obj);
            return;
        }

        Object[] tab = (Object[]) obj;

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                toString(output, tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                toString(output, tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                toString(output, tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array. Calls <code>toString(StringBuffer, Object,
     * Class)</code>.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toStringArray(StringBuffer output, Object tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        Class type = tab.getClass();
        if (!type.isArray()) {
            toString(output, tab);
            return;
        }

        toString(output, tab, type.getComponentType());
    }

    /**
     * Provides a string representation of an array. Calls <code>toString(StringBuffer, Object,
     * Class)</code>.
     *
     * @param tab the array to print
     * @return a string representation of the array
     */
    public static final String toStringArray(Object tab) {
        if (tab == null)
            return "null";

        Class type = tab.getClass();
        if (!type.isArray())
            return toString(tab);

        StringBuffer output = new StringBuffer();
        toString(output, tab, type.getComponentType());
        return output.toString();
    }

    /**
     * Provides a string representation of an array of booleans. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, boolean[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of bytes. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, byte[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of chars. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, char[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of shorts. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, short[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of ints. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, int[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of longs. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, long[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of floats. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, float[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of an array of doubles. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param tab    the array to print
     */
    public static final void toString(StringBuffer output, double[] tab) {
        if (tab == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = tab.length;
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (int i = 0; i < size; i++) {
                output.append(",");
                output.append(tab[i]);
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                output.append(tab[i]);
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                output.append(tab[size - i]);
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of a vector of objects. Uses the <code>listMax</code> and
     * <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param vector the vector of <code>Object</code> objects to print
     */
    public static final void toString(StringBuffer output, Vector vector) {
        if (vector == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = vector.size();
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (Enumeration e = vector.elements(); e.hasMoreElements(); ) {
                output.append(",");
                toString(output, e.nextElement());
            }
        } else {
            int border = size / 2;
            if (listBorder < border)
                border = listBorder;
            for (int i = 0; i < border; i++) {
                output.append(",");
                toString(output, vector.elementAt(i));
            }
            output.append(",...");
            for (int i = border; i > 0; i--) {
                output.append(",");
                toString(output, vector.elementAt(size - i));
            }
        }
        output.append(")");
    }

    /**
     * Provides a string representation of a vector of objects. Calls <code>toString(StringBuffer,
     * ...)</code>.
     *
     * @param vector the vector of <code>Object</code> objects to print
     * @return a string representation of the vector
     */
    public static final String toString(Vector vector) {
        if (vector == null)
            return "null";

        StringBuffer output = new StringBuffer();
        toString(output, vector);
        return output.toString();
    }

    /**
     * Provides a string representation of a hash table of objects. Uses the <code>listMax</code>
     * and <code>listBorder</code> variables.
     *
     * @param output a buffer to print the object into
     * @param table  the table of <code>Object</code> objects to print
     */
    public static final void toString(StringBuffer output, Hashtable table) {
        if (table == null) {
            output.append("null");
            return;
        }

        output.append("(");
        int size = table.size();
        output.append(size);
        if (listMax == -1 || size <= listMax) {
            for (Enumeration e = table.keys(); e.hasMoreElements(); ) {
                Object key = e.nextElement();
                output.append(",(");
                toString(output, key);
                output.append(",");
                toString(output, table.get(key));
                output.append(")");
            }
        } else {
            int border = size;
            if (listBorder < border)
                border = listBorder;
            Enumeration e = table.keys();
            for (int i = 0; i < border; i++) {
                Object key = e.nextElement();
                output.append(",(");
                toString(output, key);
                output.append(",");
                toString(output, table.get(key));
                output.append(")");
            }
            if (border < size)
                output.append(",...");
        }
        output.append(")");
    }

    /**
     * Provides a string representation of a hash table of objects. Calls
     * <code>toString(StringBuffer, ...)</code>.
     *
     * @param table the table of <code>Object</code> objects to print
     * @return a string representation of the table
     */
    public static final String toString(Hashtable table) {
        if (table == null)
            return "null";

        StringBuffer output = new StringBuffer();
        toString(output, table);
        return output.toString();
    }

}
