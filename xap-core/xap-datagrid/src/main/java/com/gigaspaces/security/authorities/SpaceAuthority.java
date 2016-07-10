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


package com.gigaspaces.security.authorities;

import com.gigaspaces.security.Authority;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Defines an Authority for operating on Space (stored) data, with the specified privilege. <p> The
 * <code>SpaceAuthrotiy</code>s' {@link Authority#getAuthority()} String representation format:
 * <pre>
 * <code>SpacePrivilege privilege-value [filter]</code>
 *
 * Where:
 * privilege-value = WRITE | CREATE | READ | TAKE | EXECUTE | ALTER
 * filter = RegexFilter regex |
 *          NegateRegexFilter regex |
 *          ClassFilter class-name |
 *          NegateClassFilter class-name |
 *          PackageFilter package-name |
 *          NegatePackageFilter package-name
 *
 * The privileges represent the following space operations:
 *  WRITE   - write and update operations, this grants CREATE privilege as well.
 *  CREATE  - write only operations
 *  READ    - read operations, count, notify
 *  TAKE    - take operations, clear
 *  EXECUTE - execute tasks
 *  ALTER   - clean, drop-class
 * </pre>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class SpaceAuthority implements InternalAuthority {

    /**
     * Defines operation privileges
     */
    public enum SpacePrivilege implements Privilege {
        NOT_SET,
        /**
         * write and update operations
         */
        WRITE,
        /**
         * read operations, count, notify
         */
        READ,
        /**
         * take operations, clear
         */
        TAKE,
        /**
         * execute tasks
         */
        EXECUTE,
        /**
         * clean, drop-class
         */
        ALTER,
        /**
         * write only (no update) operations
         */
        CREATE;

        @Override
        public String toString() {
            switch (this) {
                case WRITE:
                    return "Write";
                case READ:
                    return "Read";
                case TAKE:
                    return "Take";
                case EXECUTE:
                    return "Execute";
                case ALTER:
                    return "Alter";
                case CREATE:
                    return "Create";
                default:
                    return super.toString();
            }
        }
    }


    private static final long serialVersionUID = 1L;
    private final SpacePrivilege spacePrivilege;
    private final Filter filter;

    /**
     * An authority with the specified privilege.
     *
     * @param spacePrivilege a granted authority.
     */
    public SpaceAuthority(SpacePrivilege spacePrivilege) {
        this(spacePrivilege, null);
    }

    /**
     * An authority with the specified privilege.
     *
     * @param spacePrivilege granted privilege
     * @param filter         a filter on the specified privilege.
     */
    public SpaceAuthority(SpacePrivilege spacePrivilege, Filter filter) {
        this.spacePrivilege = spacePrivilege;
        this.filter = filter;
    }

    /**
     * Parses the {@link #getAuthority()} string representation of an Authority.
     *
     * @param authority the authority string to parse.
     * @return an instance of the authority represented by the authority string.
     */
    public static SpaceAuthority valueOf(String authority) {
        if (authority == null) {
            throw new IllegalArgumentException("Illegal Authority format: null");
        }

        String[] split = authority.split(Constants.DELIM);

        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!SpacePrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Privilege name in: " + authority);
        }

        SpacePrivilege spacePrivilege = SpacePrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        Filter filter = null;
        if (split.length > Constants.FILTER_POS) {
            String filterClass = split[Constants.FILTER_POS];
            String filterParams = authority.substring(authority.indexOf(split[Constants.FILTER_PARAMS_POS]));
            if (filterClass.equals(PackageFilter.class.getSimpleName())) {
                filter = new PackageFilter(filterParams);
            } else if (filterClass.equals(NegatePackageFilter.class.getSimpleName())) {
                filter = new NegatePackageFilter(filterParams);
            } else if (filterClass.equals(ClassFilter.class.getSimpleName())) {
                filter = new ClassFilter(filterParams);
            } else if (filterClass.equals(NegateClassFilter.class.getSimpleName())) {
                filter = new NegateClassFilter(filterParams);
            } else if (filterClass.equals(RegexFilter.class.getSimpleName())) {
                filter = new RegexFilter(filterParams);
            } else {
                throw new IllegalArgumentException("Unknown authority representation.");
            }
        }

        return new SpaceAuthority(spacePrivilege, filter);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return spacePrivilege.getClass().getSimpleName() + Constants.DELIM
                + spacePrivilege.name() + (filter == null ? "" : Constants.DELIM + filter);
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((getAuthority() == null) ? 0 : getAuthority().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SpaceAuthority other = (SpaceAuthority) obj;
        if (getAuthority() == null) {
            if (other.getAuthority() != null)
                return false;
        } else if (!getAuthority().equals(other.getAuthority()))
            return false;
        return true;
    }


    public Filter getFilter() {
        return this.filter;
    }

    /*
     * @see com.gigaspaces.security.authorities.InternalAuthority#getMappingKey()
     */
    public Privilege getPrivilege() {
        return spacePrivilege;
    }

    /**
     * interface for data access filtering.
     */
    public static interface Filter<T> extends Serializable {

        /**
         * The wildcard expression representing this filter.
         *
         * @return wildcard expression.
         */
        public String getExpression();

        /**
         * Tests whether or not the object is acceptable.
         *
         * @param other an object to accept.
         * @return <code>true</code> if and only if the filter accepts the the given string;
         * <code>false</code> otherwise.
         */
        public boolean accept(T other);
    }

    /**
     * interface for restrictive data access filtering.
     */
    public static interface NegateFilter<T> extends Filter<T> {

    }

    /**
     * Regular expression filter, using a regular expression as a pattern of characters that
     * describes a set of strings. Uses java.util.regex package to match all of the occurrences of a
     * pattern in an input sequence.
     */
    public static class RegexFilter implements Filter<String> {
        private static final long serialVersionUID = 1L;
        private final String regex;
        private final Pattern pattern;
        private final String expression;

        public RegexFilter(String regex) {
            this.regex = regex;
            this.pattern = Pattern.compile(regex);
            this.expression = regex.replace("\\.", ".").replace(".*", "*");
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + Constants.DELIM + regex;
        }

        public boolean accept(String otherRegex) {
            if (otherRegex == null) {
                otherRegex = "*";
            }
            return pattern.matcher(otherRegex).matches();
        }

        public String getExpression() {
            return expression;
        }
    }

    /**
     * Negate a regular expression filter.
     */
    public final static class NegateRegexFilter extends RegexFilter implements NegateFilter<String> {
        private static final long serialVersionUID = 1L;

        public NegateRegexFilter(String regex) {
            super(regex);
        }

        @Override
        public boolean accept(String otherRegex) {
            return !super.accept(otherRegex);
        }
    }

    /**
     * Package filter accepting only the defined package
     */
    public static class PackageFilter implements Filter<String> {
        private static final long serialVersionUID = 1L;
        private final String packageName;
        private final String expression;

        public PackageFilter(String packagename) {
            this.packageName = packagename;
            this.expression = packagename + ".*";
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + Constants.DELIM + packageName;
        }

        public boolean accept(String other) {
            if (other == null) return false;
            //other might be a full class name
            return other.startsWith(packageName);
        }

        public String getExpression() {
            return expression;
        }
    }

    /**
     * Package filter accepting only packages not of the defined package.
     */
    public final static class NegatePackageFilter extends PackageFilter implements NegateFilter<String> {
        private static final long serialVersionUID = 1L;

        public NegatePackageFilter(String packagename) {
            super(packagename);
        }

        @Override
        public boolean accept(String otherPackageName) {
            return !super.accept(otherPackageName);
        }
    }

    /**
     * Class filter accepting only the defined class-name.
     */
    public static class ClassFilter implements Filter<String> {
        private static final long serialVersionUID = 1L;
        private final String className;
        private final String expression;

        public ClassFilter(String className) {
            this.className = className;
            this.expression = className;
        }

        public String getClassName() {
            return className;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + Constants.DELIM + className;
        }

        public boolean accept(String otherClassName) {
            if (otherClassName == null) return false;
            return this.className.equals(otherClassName);
        }

        public String getExpression() {
            return expression;
        }
    }

    /**
     * Class filter accepting only classes not of the defined class-name.
     */
    public final static class NegateClassFilter extends ClassFilter implements NegateFilter<String> {
        private static final long serialVersionUID = 1L;

        public NegateClassFilter(String className) {
            super(className);
        }

        @Override
        public boolean accept(String otherClassName) {
            return !super.accept(otherClassName);
        }
    }

    /**
     * Convert a wildcard expression to a filter instance.
     */
    public static class WildcardExpressionToFilterConverter {
        /**
         * Convert the wildcard expression into a filter.
         *
         * @param expression expression to convert.
         * @param allow      <code>true</code> to allow matches of the expression;
         *                   <code>false</code> to deny.
         * @return a filter instance representing this expression.
         */
        public static Filter<?> convert(String expression, boolean allow) {

            if (!isPattern(expression)) {
                //expression represent a class name
                if (allow) {
                    ClassFilter filter = new ClassFilter(expression);
                    return filter;
                }
                NegateClassFilter filter = new NegateClassFilter(expression);
                return filter;
            }

            if (isPackagePattern(expression) && !isRegexPattern(expression)) {
                String packageName = expression.substring(0, expression.length() - 2);
                if (allow) {
                    PackageFilter filter = new PackageFilter(packageName);
                    return filter;
                }
                NegatePackageFilter filter = new NegatePackageFilter(packageName);
                return filter;
            }

            // expression represents a regular expression
            return convertToRegex(expression, allow);
        }

        /**
         * Convert the wildcard expression into a regex expression, since we found that this is a
         * faster implementation even for fully matched strings.
         *
         * @param expression expression to convert.
         * @param allow      <code>true</code> to allow matches of the expression;
         *                   <code>false</code> to deny.
         * @return a filter instance representing this expression.
         */
        public static Filter<?> convertToRegex(String expression, boolean allow) {
            String toRegex = expression.replace(".", "\\.").replace("*", ".*");
            if (allow) {
                RegexFilter filter = new RegexFilter(toRegex);
                return filter;
            }
            NegateRegexFilter filter = new NegateRegexFilter(toRegex);
            return filter;
        }

        /*
         * isPattern && contains a \
         * No need to check if contains: '.' and '*'
         */
        private static boolean isRegexPattern(String expression) {
            return expression.indexOf('\\') != -1;
        }

        /*
         * isPattern && a ".*" only at the end
         */
        private static boolean isPackagePattern(String expression) {
            return expression.endsWith(".*") && expression.indexOf('*') == expression.length() - 1;
        }

        /*
         * consists of '*' or '?'
         */
        private static boolean isPattern(String expression) {
            return (expression.indexOf('*') != -1 || expression.indexOf('?') != -1);
        }
    }
}
