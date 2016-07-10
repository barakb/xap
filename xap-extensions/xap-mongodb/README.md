[![Build Status](https://api.travis-ci.org/Gigaspaces/mongo-datasource.png)](http://travis-ci.org/Gigaspaces/mongo-datasource)

Overview
================

Implementation for XAP mongodb persistency SpaceDataSource and SpaceSynchronizationEndpoint

Prerequisites
=============
* [MongoDB](http://www.mongodb.org/)
  * [Download](http://www.mongodb.org/downloads) and follow the [installation instructions](http://docs.mongodb.org/manual/installation/)
  * Create environment variable `MONGO_HOME` = `mongo installtion directory`
  * Add `MONGO_HOME\bin` to `PATH` environment variable

Build
=====

* Clone the project: `git clone https://github.com/Gigaspaces/mongo-datasource.git`
* Navigate to the `mongo-datasource` project directory
* Build project 
  * without tests: `mvn clean install -DskipTests`
  * with tests: `mvn clean surefire:test install`

> ##### Notes #####

> * eclipse users uses m2e plugin sometimes miss synchronization its recommended to right click on 
  mongodb-datasource project and from the menu [Maven]-> [Update project]


Repositories
============
	<repositories>
		<repository>
			<id>org.openspaces</id>
			<name>OpenSpaces</name>
			<url>http://maven-repository.openspaces.org</url>
		</repository>

	</repositories>

Dependencies
============
    		<dependency>
			<groupId>org.gigaspaces</groupId>
			<artifactId>xap-openspaces</artifactId>
			<version>9.7.0-SNAPSHOT</version>
		</dependency>

		<!-- mongodb java driver -->
		<dependency>
			<groupId>org.mongodb</groupId>
			<artifactId>mongo-java-driver</artifactId>
			<version>3.2.0</version>
		</dependency>
	

		<dependency>
			<groupId>org.antlr</groupId>
			<artifactId>antlr4-runtime</artifactId>
			<version>4.0</version>
		</dependency>

