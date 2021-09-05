---
layout: post
title: "Intro to Massively Parallel Processing"
date: 2021-09-13
category: Data Engineering
published: False
---

Massively Parallel Processing(MPP) databases have been around for decades, but their cost and the complexity of managing them has dropped tremendously in the last decade. The only option until recently was to self-host these databases, but more recently, they have migrated to the cloud.

It is an important part of any modern data-warehouse and database architecture and its one of the reasons that analytics is possible across petabytes of data, famously known as Big-Data scenarios. They help companies find insight and create business value. Hence, its imperative that anyone working with tools like the ones mentioned below, know about them and get one step closer to using them better. 
[img]

## Definitions

Before we move any further, we need a basic understanding of what we are going to discuss.

#### Data Warehouse:

A data warehouse centralizes and consolidates large amounts of **structured** data from multiple sources. It is designed to enable and support business intelligence (BI) activities, especially analytics.

[img]

#### Massively Parallel Processing:

It is a means of crunching huge amounts of data by distributing the processing over hundreds or thousands of processors. The problem being worked on is divided into many pieces, which are processed simultaneously by the multiple systems.

## Deeper into MPP Architecture

- Each processor uses its own OS and memory.
- They communicate with each other using some form of messaging interface.
- MPP can be setup with a shared nothing or shared disk architecture, Like in the image below.

[img]

In a shared nothing architecture, there is no single point of contention(meaning, there is no conflict among multiple processes over shared resources) across the system and the nodes do not share memory or disk storage. Data is horizontally partitioned across nodes such that each node has a subset of rows from each table in the database. Each node then processes only the rows in its own disks.

Systems on this architecture can achieve massive scale as there is no single bottleneck to slow down the system.

Below is a labelled diagram of MPP architecture:

[img]

**Leader node:** This is the brain of the architecture which acts like a front end as it interacts with all the applications and connections. It develops the query execution plan and coordinates the execution of code in each compute node parallelly. 

**Compute node:** It provides the computational power to executes the code. It then sends the intermediate results back to the leader node for final aggregation. Some queries require data movement to ensure the parallel queries return accurate results.

**Storage:** All the data is stored here in a distributed style to leverage parallel processing for better query performance. You can choose which shard pattern to use to distribute the data when you define the table. 

**Cluster:** The whole infrastructure is called a cluster. FYI, In snowflake one can provision multiple such clusters.

Nowadays, MPP architecture is used to process relational and non-relational data.

## Columnar storage

A data warehouse is a OLAP (Online Analytical Processing) system and using columnar storage is beneficial in such systems. We will try to understand this with an example.

Consider the below table

| ID   | Name          | Address                 | Country   | Age  |
| ---- | ------------- | ----------------------- | --------- | ---- |
| 1    | Raj Kumar     | 23 M G Road             | India     | 43   |
| 2    | Neeraj Gupta  | 42/1 Shakespeare Sarani | India     | 26   |
| 3    | Satish Mishra | 1936 Lilydale Drive     | Australia | 33   |

 In a row-oriented database, data is stored in the following way:

```reStructuredText
1,Raj Kumar,23 M G Road,India,43;2,Neeraj Gupta,42/1 Shakespeare Sarani,India,26;3,Satish Mishra,1936 Lilydale Drive,Australia,33;
```

And in a columnar database, data is stored as :

```reStructuredText
1,2,3;Raj Kumar,Neeraj Gupta,Satish Mishra;23 M G Road,42/1 Shakespeare Sarani,1936 Lilydale Drive;India,India,Australia;43,26,33;
```

Here, some of our queries could become really fast. Imagine, for example, that you wanted to know the average age of all your users. Instead of looking the age of each row by row, we can simply jump to the area where the "age" data is stored and read just the data you need. So, columnar storage lets you skip over all the non-relevant data very quickly. Aggregation queries become really fast, which is what we do in analytical systems. 

If we were to query for looking up user-specific values only, row oriented databases perform those queries much faster. Additionally, writing new data could take more time in columnar storage. Hence, this type of storage is not meant for OLTP systems as they typically perform more of insertion, updation and/or deletion of small amounts of data.

to reiterate, **columnar storage is used under the following conditions:**

- You have a OLAP system
- You query on less number of columns
- You have very large tables with millions and billions of rows.

And when you use this form of storage under the following conditions, **you have the following advantages**: 

- Improved query performance
- Support compression(which make it even faster) like AZ64, LZO compression, etc.
- Reduce storage cost

Hence data warehouses convert your row-oriented database tp columnar database. 