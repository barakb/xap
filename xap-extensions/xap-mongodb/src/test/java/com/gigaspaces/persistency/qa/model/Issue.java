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

package com.gigaspaces.persistency.qa.model;

import java.util.Date;

/**
 * An Issue (simplification of an Issue in JIRA) which can be updated by voting ({@link #vote()} or
 * changing its priority directly ( {@link Issue#setPriority(src.main.java.com.gigaspaces.stest.model.data.issue.Issue.Priority)}
 *
 * This Issue creates its UID based on its 'key'.
 *
 * Indexed by: key, created, updated, reporter, priority MetaDataEntry, Cloneable, Comparable
 *
 * @author moran
 */
public interface Issue extends Comparable<Issue>, Cloneable {
    /**
     * @return the unique key of this issue.
     */
    public Integer getKey();

    /**
     * @return reporter of this issue.
     */
    public User getReporter();

    /**
     * @return when this issue was created - default now.
     */
    public Date getCreated();

    /**
     * @return when this issue was last updated.
     */
    public Date getUpdated();

    /**
     * @return number of votes for this issue - default zero.
     */
    public Integer getVotes();

    /**
     * @return priority of this issue - default trivial
     */
    public Priority getPriority();

    /**
     * @return votes string representation - used for regular expressions and alike
     */
    public String getVotesRep();

    /**
     * Sets the unique key of this issue.
     *
     * @param key unique key of this issue.
     */
    public void setKey(Integer key);

    /**
     * Sets the number of votes for this issue - default zero.
     *
     * @param votes number of votes for this issue.
     */
    public void setVotes(Integer votes);

    /**
     * prioritize this issue, regardless of number of votes.
     *
     * @param p priority to assign for this issue.
     */
    public void setPriority(Priority p);

    /**
     * votes for this issue - increases priority every 5 votes unless current priority is higher
     * than voted priority. 'updated' time is modified to now.
     *
     * Priority.TRIVIAL 0-5 votes Priority.MINOR 5-10 votes Priority.MEDIUM 10-15 votes
     * Priority.MAJOR 15-20 votes Priority.CRITICAL 20-25 votes Priority.BLOCKER 25+ votes
     *
     * @return previous priority.
     */
    public Priority vote();

    /**
     * Shallow clones this Issue. Same as calling {@link Object#clone()} encapsulating the throws
     * and return value.
     *
     * @return shallow clone of this issue.
     */
    public Issue shallowClone();

    /**
     * Deep clones this Issue, for fields: key, reporter, votes and EntryInfo. Other fields are
     * immutable.
     *
     * @return deep clone of this issue.
     */
    public Issue deepClone();
}