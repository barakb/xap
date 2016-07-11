# Hello World Example

This example has two counter parts: client and server. The client, a simple main called HelloWorld.java updates a data-grid with "Hello" and "World!" data entities and then reads them back. 
The `HelloWorld` main accepts the following arguments: `-name` {data-grid name} `-mode` {embedded,remote}

## Message.java

A Plain Old Java Object (POJO) is the entity behind the updates to the data-grid. 
It constitutes of getters and setters for the 'msg' field, and a `@SpaceId` for uniqueness (similar to a Map key).

### Annotations

Additional annotations may be applied - here are a couple:

- A `@SpaceRouting` annotation can be applied to any field to allow routing to partition instances. If not specified, `@SpaceId` will act as the routing field.
- A `@SpaceIndex` annotation can be applied to any field to allow indexing. `@SpaceId` is by default indexed.

## HelloWorld.java

This main class acts as the client. It can either start a single data-grid instance (embedded) in it's JVM, or connect to an existing (remote) data-grid (by it's name).

## Running the Example - Embedded
Import Maven `examples/hello-world/pom.xml` example into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` embedded)

This will start an embedded data-grid followed by write and read of Message entities.

### output
```
Created embedded data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-1](https://cloud.githubusercontent.com/assets/8696298/16544698/be661c64-4118-11e6-8e5b-d031bb6b40ea.png)

## Running the Example - Remote

To connect to a *remote* data-grid, first use the `space-instance.{sh,bat}` script to launch a data-grid.

From the ${XAP_HOME}/bin directory, run:

-  ./space-instance.sh `-name` myDataGrid

Import Maven `examples/hello-world/pom.xml` into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` remote)
> use `myDataGrid` same as the `-name` argument passed to `space-instance.{sh.bat}`

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-1r](https://cloud.githubusercontent.com/assets/8696298/16724700/b695f4ce-475c-11e6-9617-00df8b561a52.png)

## Running the Example - Remote (with 2 partitions)

Each partition instance is loaded separately, as follows:

1. Specify `total_members=2` for two partitions
2. Specify `id=1` or `id=2` for each partition instance

From the ${XAP_HOME}/bin directory, run:

-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2 **id=1**
-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2 **id=2**

This will simulate a data-grid of 2 partitioned instances (without backups).

Import Maven `examples/hello-world/pom.xml` example into your IDE of choice as a maven project.
Launch the `HelloWorld` main (arguments: `-name` myDataGrid `-mode` remote)

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-2](https://cloud.githubusercontent.com/assets/8696298/16428814/4508afd4-3d7c-11e6-9ed2-5de2b12ebb4e.png)

## Running the Example - Remote (with backups for each partition)

Each partition instance can be assigned a backup, as follows:

1. Specify `total_members=2,1` for two partitions, each with a single backup.
2. Specify `backup_id=1` to load the backup instance of partition id=1 or id=2

**First partition:**

-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2,1 id=1
-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2,1 id=1 **backup_id=1**

**Second partition:**

-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2,1 id=2
-  ./space-instance.sh `-name` myDataGrid `-cluster` schema=partitioned total_members=2,1 id=2 **backup_id=1**


The Example should be run in the same manner as before - Launch the `HelloWorld` (arguments: `-name` myDataGrid `-mode` remote).

### output
```
Connected to remote data-grid: myDataGrid
write - 'Hello'
write - 'World!'
read - ['Hello', 'World!']
```

![helloworld-3](https://cloud.githubusercontent.com/assets/8696298/16428817/48ba0650-3d7c-11e6-83f9-69c0598610eb.png)
