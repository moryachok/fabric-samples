# Delta Table Maintanance

## Introduction

In this notebook, you will gain practical knowledge of **Delta Lake table operations**. Through hands-on examples, we will explore Delta's powerful features, including **metadata management**, **time-travel** across table versions, and essential maintenance commands like **optimize** and **vacuum**. By the end of this tutorial, you will have a deep understanding of how to manage Delta tables effectively, when to run maintenance operations, and why they are important.

### Key Learning Objectives:
- **Explore Delta Table Metadata**: Learn how to retrieve and interpret metadata information, such as table history, to understand changes made over time.
- **Time Travel in Delta**: Master the ability to query previous versions of a Delta table using Delta's time-travel capabilities.
- **Restore Table to Specific Version**: Practice restoring a Delta table to a previous version and understand the scenarios where this feature is critical.
- **Optimize Delta Table**: Improve query performance using the `OPTIMIZE` command.
- **Vacuum Command**: Reclaim storage space by removing data files from previous table versions using the `VACUUM` command.
  
We will build up knowledge gradually, starting from basic Delta table operations and progressing to advanced table management techniques. By the end of this notebook, you will have a solid understanding of Delta table operations and best practices for maintaining a healthy data lake.

### Prerequisites
This notebook is designed to work inside [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric). Although the commands used throughout the notebook are plain Spark SQL and should work in other Spark environments (e.g., Databricks, Apache Spark, etc.), slight modifications might be required depending on the environment. If you're using Microsoft Fabric, the notebook should run without issues.

Before you start running this notebook be sure to [connect your notebook to the existing lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#connect-lakehouses-and-notebooks) in your Microsoft Fabric environment.


## Exploring Delta Table metadata

We will start by creating a sample table and ingesting some data. Notice the `USING delta` at the end of SQL statement that ensures that this table is created in Delta format.

```sql
CREATE TABLE IF NOT EXISTS delta_test_table (
    id INT,
    name STRING,
    age INT,
    city STRING
)
USING delta
```

```sql
INSERT INTO delta_test_table VALUES
    (1, 'John Doe', 28, 'New York'),
    (2, 'Jane Smith', 34, 'Los Angeles'),
    (3, 'Mike Johnson', 45, 'Chicago'),
    (4, 'Sara Davis', 29, 'Houston'),
    (5, 'Chris Lee', 31, 'San Francisco');
```

Now that you created a table and inserted some data run `SELECT` to validate that the table indeed has the data in it.

```sql
SELECT * FROM delta_test_table;
```

### Table History 

Delta creates a table version on every transaction and operation. Run `DESCRIBE HISTORY` to see how the table evolved over time. 

```sql
DESCRIBE HISTORY delta_test_table;
```

From the `DESCRIBE HISTORY delta_test_table` output we can see that:
1. `delta_test_table` has 2 versions -> `id=0` and `id=1`.
2. By exploring `operation` column you can see that version `id=0` is the Table creation and version `id=1` created when you ran the `INSERT` query. 
3. Another interesting column is `operationMetrics` where you can see how many files and rows were added by the `INSERT` query.
```json
{"numFiles":"1","numOutputRows":"5","numOutputBytes":"2232"}
```

For detailed description of each `HISTORY` column please visit [Delta Lake documentation](https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history).

Now run `DESCRIBE DETAIL` query to see more details about the current table state. 

```sql
DESCRIBE DETAIL delta_test_table;
```

`DESCRIBE DETAIL <TABLENAME>` provides you with additional information related to the table current state.
You can see format is set to `delta` and when this table was created (`createdAt`), when it was last modified (`lastModified`) as well as information about column(s) used to partition the table. In this example `partitionColumns` is empty because I haven't used partitioning. I want you to pay attention to two columns:
1. `numFiles` - Number of the files in the latest version of the table.
2. `sizeInBytes` - The size of the latest snapshot of the table in bytes.

You can also look into `properties` column to see table properties, but I prefer to use another SQL command for this `SHOW TBLPROPERTIES <TABLENAME>`

```sql
SHOW TBLPROPERTIES delta_test_table;
```

With `SHOW TBLPROPERTIES` you can see what table properties you have on the table and learn about each and every one of them.
I will just mention the `delta.parquet.vorder.enabled` which is a write time optimization to the parquet file format that enables lightning-fast reads under the Microsoft Fabric compute engines, such as Power BI, SQL, Spark, and others. It's enabled by default for all the Managed Delta tables in Microsoft Fabric.
You can learn more about V-Order [here](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order?tabs=sparksql).

## Time-travel and Rollbacks

Time-travel is one of the key features of Delta as it allows you to explore your data in a past. It can be very helpful as a mechanism to recover from unintentional updates or deletes. 

Let's first append some more data into the table.

```sql 
INSERT INTO delta_test_table VALUES
    (6, 'Alice Brown', 40, 'Seattle'),
    (7, 'Tom White', 27, 'Denver'),
    (8, 'Emily Clark', 35, 'Boston'),
    (9, 'David Wilson', 50, 'Miami'),
    (10, 'Sophia Martinez', 22, 'Dallas');
SELECT * from delta_test_table;
```

You can see that the table now consists of 10 rows which as you remember appended in two insert statements. Therefore I expect to see a new table version in a table history.
Run `DESCRIBE HISTORY`.

```sql
DESCRIBE HISTORY delta_test_table;
```

Indeed the History table now has version `id=2` with `operation=WRITE` and `operationMetrics` column that shows that `INSERT` created one file(`numFiles`) and five new rows(`numOutputRows`).

Run `DESCRIBE DETAIL` to see what changed there.

```sql
DESCRIBE DETAIL delta_test_table;
```

`DESCRIBE DETAIL` shows you that the table now consist of 2 data files (`numFiles`) with total size of 4436 bytes (`sizeInBytes`).
This compared to `numFiles=1` and `sizeInBytes=2232` before the second `INSERT`.

Now you can use time-travel feature to explore the table's previous versions.
Let's see the data as it was before the second `INSERT`.

```sql
SELECT * FROM delta_test_table VERSION AS OF 1;
```

`VERSION AS OF 1` will present the data for table version `id=1`. The `id` of specific version can be found in `DESCRIBE HISTORY <TABLENAME>`.

As you can see the `SELECT` query with `VERSION AS OF 1` shows only 5 rows, this was the table's data before the second `INSERT`.
You can time-travel to the specific version or specific point-in-time. To learn more about time-travel [visit delta documentation](https://learn.microsoft.com/en-us/azure/databricks/delta/history#delta-time-travel-syntax).

Another powerful feature of delta is the ability to **restore** table to it's previous state.
Assuming you've made that second `INSERT` unintentionally and now you want to get rid of this new data.
You can restore your table to specific version from the `HISTORY` or to point-in-time. 
Let's see how it works.

```sql
RESTORE TABLE delta_test_table TO VERSION AS OF 1;
```

The `RESTORE` output shows that during this operation you've removed 1 file (`num_removed_files`) with total size of 2204 bytes (`removed_files_size`). 

Now when you run `SELECT` on the table you should get only 5 rows.

```sql
SELECT * FROM delta_test_table;
```

After running `RESTORE` the current table version is `id=1` with total of 5 rows and 1 data file.

Now explore the `HISTORY` table to see how `RESTORE` operation is presented there.

```sql
DESCRIBE HISTORY delta_test_table;
```

Interesting, a new row was added with `id=3` and `operation=RESTORE`. 

When you restore table to a specific version in a past, Delta will log it as a new version so you could still keep track on the table changes that being made. Restoring to `version=1` will **not** discard any subsequent versions. This let you still time-travel back and forth if needed.

Let's run `RESTORE` again to return to version `id=2`.

```sql
RESTORE TABLE delta_test_table TO VERSION AS OF 2;
```

Great, you can see the restored 1 file with total size of 2204 bytes. The same exact numbers were removed by previous `RESTORE` command.
Run `SELECT` to validate the data is fully available again.

```sql
SELECT * FROM delta_test_table;
```

Great, you have 10 rows again! Your table is restored to a version `id=2`.

In the next module you will learn about table maintenance operations.

## Table Maintenance

### Optimize

In the context of data lake optimizations, data file size plays a crucial role. Without diving too deep into the topic, it's important to note that you should aim for a smaller number of larger files. The optimal file size is typically around 500MB to 1GB per file. These sizes can significantly improve query performance.

In this section you will run Delta `OPTIMIZE` command to consolidate small files. You can also run this command from the Microsoft Fabric Lakehouse console, but sometimes you would prefer to run `OPTIMIZE` command right after the ingestion as part of your data pipeline. To learn more about Delta table maintanance in Microsoft Fabric visit [this link](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-table-maintenance).

Let's first ingest some more data.



```sql
INSERT INTO delta_test_table VALUES
    (11, 'Liam Walker', 38, 'Portland'),
    (12, 'Olivia Turner', 26, 'Austin'),
    (13, 'Noah Harris', 41, 'Phoenix'),
    (14, 'Emma Scott', 33, 'Las Vegas'),
    (15, 'Ava Robinson', 29, 'San Diego');

INSERT INTO delta_test_table VALUES
    (16, 'Isabella Hall', 36, 'Atlanta'),
    (17, 'Mason King', 42, 'Orlando'),
    (18, 'Sophia Wright', 27, 'Denver'),
    (19, 'James Green', 30, 'Salt Lake City'),
    (20, 'Mia Baker', 23, 'Dallas');

```

Run `DESCRIBE DETAIL` to see the current table state.

```sql
DESCRIBE DETAIL delta_test_table;
```

You can see that the table consist of 4 data files (`numFiles`) with total size of 8920 bytes (`sizeInBytes`). The files are too small. Let's improve that with `OPTIMIZE`.

```sql
OPTIMIZE delta_test_table;
```

Now run `DESCRIBE DETAIL` to see what changed.

```sql
DESCRIBE DETAIL delta_test_table;
```

Now your table consist of **single file with total size of 2728 bytes**. This is almost **4 times** improvement in terms of storage reducted. 

For larger tables with millions of rows periodic execution of `OPTIMIZE` could lead to significant improvements in query performance.

Let's run `SELECT` query to validate that the data is fully present.

```sql
SELECT * FROM delta_test_table order by id;
```

In this section you've learned how to check your table state with `DESCRIBE DETAIL` to see if your table will benefit from the `OPTIMIZE` command.

### Vacuum

VACUUM removes old data files no longer referenced by a Delta table log. Delta keeps all the history of all transactions and operations such as writes, updates, deletes as well as optimize and restore. 

In the previous module you ran `OPTIMIZE` command to consolidate four small data files into a larger one. This greatly improves the query performance but the way Delta works is that it still keeps those four small files stored in the data lake even though the current table is no longer reference them. This is done to allow you to go back in time and restore previous version of the table as we learned previously.

Run `DESCRIBE HISTORY` to see all the table versions.


```sql
DESCRIBE HISTORY delta_test_table;
```

You can see that the last table version with `id=7` is your `OPTIMIZE` command.

However the unreferenced four files are still stored in the lake and you will still pay for them. 
`VACUUM` will delete those files and help you to optimize your storage costs.

You should be cautious regarding using `VACUUM`. Effectively you optimize your storage cost, but sacrifice the ability to restore table to previous versions. 
The default retention period in Microsoft Fabric for your lakehouse tables is **7 days**. It means that even if you run `VACUUM` command, it will only affect table versions that are older than 7 days. You can change this default based on your organizational requirements. 

For the sake of the experiment I will run `VACUUM` with retention of 0 days. It will remove all the unreferenced files of all the previous table versions except of the current one.

**Setting retention to 0 days is NOT RECOMMENDED**.

```python
# this is required command to run before VACUUM with 0 days.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

```sql
VACUUM delta_test_table RETAIN 0 HOURS;
```

`VACUUM` command finished successfully, now check that the data is still present in a table.

```sql
SELECT * FROM delta_test_table order by id;
```

All the 20 rows are still present. Now run `DESCRIBE HISTORY` to see the effect of `VACUUM`.

```sql
DESCRIBE HISTORY delta_test_table;
```

There are two new table versions with operation `VACUUM START` and `VACUUM END`. Notice VACUUM START `operationParameters` column that shows that you've changed the retention default from 7 days to 0.
```json
{"retentionCheckEnabled":"false","defaultRetentionMillis":"604800000","specifiedRetentionMillis":"0"}
```
Next explore `operationMetrics` column for `VACUUM END`. The value shows that 4 files have been deleted.
```json
{"numDeletedFiles":"4","numVacuumedDirectories":"1"}
```

You have successfully vacuumed the table and you no longer need to pay for storage of 4 unreferenced data files. 
At the same time you no longer has an option to restore table to previous versions. 

If you try to run the `RESTORE` command you will get an error of _Missing Files_.

```sql
RESTORE TABLE delta_test_table TO VERSION AS OF 2;
```



### Recap

In this module you've learned about VACUUM command. Dont forget about the tradeoff between the time-travel and restore features and reclaiming the storage space with VACUUM.

To learn more about table maintanance and VACUUM visit Microsoft Fabric [documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-table-maintenance).

## Cleanup

```sql
DROP TABLE IF EXISTS delta_test_table;
```

