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

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceVersion;

import java.io.Serializable;
import java.util.Date;

/**
 * A representation of a JIRA Issue (as POJO)
 */
@SpaceClass
public class SerializableIssuePojo
        implements Issue, Serializable {
    /**
     * default serialization version uid
     */
    private static final long serialVersionUID = 1L;

    /**
     * unique key of this issue - required
     */
    private Integer key;
    /**
     * reporter of this issue - required
     */
    private User reporter;
    /**
     * when this issue was created - default now
     */
    private Date created;
    /**
     * when this issue was last updated - default now
     */
    private Date updated;
    /**
     * number of votes for this issue - default zero
     */
    private Integer votes;
    /**
     * priority of this issue - default trivial
     */
    private Priority priority;
    /**
     * votes string representation - used for regular expressions and alike
     */
    private String votesRep;

    private int version;


    /**
     * null-task default constructor; Doesn't generate UID.
     */
    public SerializableIssuePojo() {
    }

    /**
     * Constructor with required fields. All other fields will be set with defaults. Reporter will
     * be set with the current System defined user.
     *
     * @param key Issue unique key.
     */
    public SerializableIssuePojo(Integer key) {
        this(key, getCurrentSystemUser());
    }

    /**
     * Constructor with required fields. All other fields will be set with defaults.
     *
     * @param key      Issue unique key.
     * @param reporter name of the User whom reported this issue
     * @throws IllegalArgumentException if reporter is null.
     */
    public SerializableIssuePojo(Integer key, String reporter) {
        this.key = key;
        this.reporter = new User(reporter);

        //defaults
        long now = System.currentTimeMillis();
        created = new Date(now);
        updated = new Date(now);
        priority = Priority.TRIVIAL;
        votes = 0;
        votesRep = String.valueOf(votes);
    }


    /**
     * Returns the currently defined system user, mapped to the system property "user.name".
     *
     * @return user name; default "anonymous"
     */
    private static String getCurrentSystemUser() {
        return System.getProperty("user.name", "anonymous");
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#vote()
     */
    public Priority vote() {
        updated = new Date(System.currentTimeMillis()); //now

        ++votes;
        votesRep = String.valueOf(votes);
        Priority votedPriority = Priority.TRIVIAL;
        Priority previous = priority;

        switch (votes) {
            case 5:
                votedPriority = Priority.MINOR;
                break;
            case 10:
                votedPriority = Priority.MEDIUM;
                break;
            case 15:
                votedPriority = Priority.MAJOR;
                break;
            case 20:
                votedPriority = Priority.CRITICAL;
                break;
            case 25:
                votedPriority = Priority.BLOCKER;
                break;
        }

        if (votedPriority.ordinal() > priority.ordinal())
            priority = votedPriority;

        return previous;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#setPriority(com.gigaspaces.common_data.issue.IssueMetaDataEntry.Priority)
     */
    public void setPriority(Priority p) {
        priority = p;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#getPriority()
     */
    public Priority getPriority() {
        return priority;
    }


    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#getCreated()
     */
    @SpaceIndex
    public Date getCreated() {
        return created;
    }

    /**
     * Sets when this issue was created;
     *
     * @param created when this issue was created - default now.
     */
    public void setCreated(Date created) {
        this.created = created;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#getKey()
     */
    @SpaceId
    public Integer getKey() {
        return key;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#setKey(java.lang.Integer)
     */
    public void setKey(Integer key) {
        this.key = key;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#getReporter()
     */
    @SpaceIndex
    public User getReporter() {
        return reporter;
    }

    /**
     * Sets the reporter of this issue.
     *
     * @param reporter reporter of this issue.
     */
    public void setReporter(User reporter) {
        this.reporter = reporter;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#getUpdated()
     */
    @SpaceIndex
    public Date getUpdated() {
        return updated;
    }

    /**
     * Sets when this issue was last updated.
     *
     * @param updated when this issue was last updated.
     */
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.data.issue.Issue#getVotes()
     */
    @SpaceIndex
    public Integer getVotes() {
        return votes;
    }

    /*
     * @see com.gigaspaces.data.issue.Issue#getVotesRep()
     */
    @SpaceIndex
    public String getVotesRep() {
        return votesRep;
    }

    /**
     * Sets a String representation of 'votes'.
     *
     * @param votesRep votes rep.
     */
    public void setVotesRep(String votesRep) {
        this.votesRep = votesRep;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#setVotes(java.lang.Integer)
     */
    public void setVotes(Integer votes) {
        this.votes = votes;
        votesRep = String.valueOf(votes);
    }

    /**
     * POJO version attribute
     *
     * @return the current version of this POJO.
     */
    @SpaceVersion
    public int getVersion() {
        return version;
    }

    /**
     * Sets the POJO version attribute
     *
     * @param version the version of this POJO.
     */
    public void setVersion(int version) {
        this.version = version;
    }


    //
    // ---------- Object constructs ----------
    //

    /* @see java.lang.Object#toString() */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\t KEY-" + getKey());
        sb.append("\t reporter: " + (getReporter() == null ? "Unassigned" : getReporter()));
        sb.append("\t votes: " + getVotes());
        sb.append("\t priority: " + getPriority());
        sb.append("\t created: " + getCreated());
        sb.append("\t updated: " + getUpdated());
        return sb.toString();
    }

    /* @see java.lang.Object#hashCode() */
    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    /*
     * @see java.lang.Object#equals(java.lang.Object)
     */
    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#equals(java.lang.Object)
	 */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SerializableIssuePojo))
            return false;

        SerializableIssuePojo otherIssue = (SerializableIssuePojo) obj;

        //verify key
        if (this.getKey() == null && otherIssue.getKey() != null)
            return false;
        else if (this.getKey() != null && !this.getKey().equals(otherIssue.getKey()))
            return false;

        //verify reporter
        if (this.getReporter() == null && otherIssue.getReporter() != null)
            return false;
        else if (this.getReporter() != null && !this.getReporter().equals(otherIssue.getReporter()))
            return false;

        //verify votes
        if (this.getVotes() == null && otherIssue.getVotes() != null)
            return false;
        else if (this.getVotes() != null && !this.getVotes().equals(otherIssue.getVotes()))
            return false;

        //verify priority
        if (this.getPriority() == null && otherIssue.getPriority() != null)
            return false;
        else if (this.getPriority() != null && !this.getPriority().equals(otherIssue.getPriority()))
            return false;

        //verify created
        if (this.getCreated() == null && otherIssue.getCreated() != null)
            return false;
        else if (this.getCreated() != null && !this.getCreated().equals(otherIssue.getCreated()))
            return false;

        //verify updated
        if (this.getUpdated() == null && otherIssue.getUpdated() != null)
            return false;
        else if (this.getUpdated() != null && !this.getUpdated().equals(otherIssue.getUpdated()))
            return false;

        //verify votesRep
        if (this.getVotesRep() == null && otherIssue.getVotesRep() != null)
            return false;
        else if (this.getVotesRep() != null && !this.getVotesRep().equals(otherIssue.getVotesRep()))
            return false;

        //all properties equal
        return true;
    }

    /**
     * Compares to Issue by their key ordering.
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Issue otherIssue) {
        return this.getKey().compareTo(otherIssue.getKey());
    }

    /**
     * @see java.lang.Object#clone()
     * @see #shallowClone()
     * @see #deepClone()
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#shallowClone()
     */
    public Issue shallowClone() {
        try {
            Issue shallow = (Issue) clone();
            return shallow;

        } catch (CloneNotSupportedException e) {
            //can't happen
        }

        return null;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.common_data.issue.Issue#deepClone()
     */
    public Issue deepClone() {
        try {
            SerializableIssuePojo deep = (SerializableIssuePojo) clone();
            deep.key = new Integer(key);
            deep.reporter = new User(reporter.getUsername());
            deep.votes = new Integer(votes);
            deep.votesRep = String.valueOf(deep.votes);

            return deep;

        } catch (CloneNotSupportedException e) {
            //can't happen
        }

        return null;
    }
}
