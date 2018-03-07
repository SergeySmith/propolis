# Propolis for Apache Hive

Propolis project is a collection of custom Hive UDFs that were not found (to the best of our current knowledge) in Hive.

## Quickstart
* Clone the sources and build the jar file:
```bash
$ git clone https://github.com/SergeySmith/propolis.git 
$ cd propolis
$ mvn clean package
# or
$ mvn clean package -DskipTests
```
* Add jar to distributed cache:

for Hive CLI
```bash
hive> add jar /path/to/propolis.jar;
# e.g.
hive> add jar ./target/propolis-1.0-SNAPSHOT.jar;
```
for Beeline (Hive Server 2)
```bash
# copy jar file to HDFS
$ hdfs dfs -copyFromLocal ./target/propolis-1.0-SNAPSHOT.jar <your_path>
beeline> add jar /path/to/propolis.jar;
```
* Register the udfs

e.g.
```sql
drop temporary function if exists counter;
create temporary function counter
  as 'org.hive.propolis.CounterUDAF'
;
```
see the description:
```sql
describe function extended <function_name>;
```

## Description
* Intersect file UDF
```sql
drop temporary function if exists intersect_file;
create temporary function intersect_file
  as 'org.hive.propolis.IntersectFileUDF'
;
```
intersect_file(array<str>, filename) – Removes all elements of the list that do not appear in the file
Similar to in_file Hive UDF, but for arrays

* Subtract file UDF
```sql
drop temporary function if exists subtract_file;
create temporary function subtract_file
  as 'org.hive.propolis.SubtractFileUDF'
;
```
subtract_file(array<str>, filename) – Removes all elements of the list that appear in the file

* Sum up (merge) Maps UDAF
```sql
drop temporary function if exists merge_maps;
create temporary function merge_maps
  as 'org.hive.propolis.MergeMapsUDAF'
;
```
merge_map(input_map Map<U, Numeric>): Map<U, Numeric> – aggregates (merges) Maps for each group: sums up all the values for the same key, run describe extended for more details

* Split string UDF
```sql
drop temporary function if exists split_str;
create temporary function split_str
  as 'org.hive.propolis.SplitStringUDF'
;
```
split_str (String str, String pattern, Integer position): Array<String> – split string in Java way (without regexp)

* Cast array<binary> to array<string> UDF
```sql
drop temporary function if exists cast_binary;
create temporary function cast_binary
  as 'org.hive.propolis.ToStringArrayUDF'
;
```
cast_binary(Array<Binary>): Array<String> – function to cast array<binary> to array<string>

* Counter UDAF
```sql
drop temporary function if exists counter;
create temporary function counter
  as 'org.hive.propolis.CounterUDAF'
;
```
Count occurrences of each input value and return a Map<value, count> (like Python Counter class).

* Average Arrays element-wise
```sql
drop temporary function if exists avg_arrays;
create temporary function avg_arrays
  as 'org.hive.propolis.AvgArraysUDAF'
;
```
Returns a new array with each element averaged.

* Simple UDF to get rid of get parameters (queries) from URL string
```sql
drop temporary function if exists cut_url;
create temporary function cut_url
  as 'org.hive.propolis.CutUrlQueryUDF'
;
```
Returns URL without get-parameters

* Avg columns element-wise
```sql
drop temporary function if exists avgs;
create temporary function avgs
  as 'org.hive.propolis.MultipleAvgUDAF'
;
```
Returns a new array with each element averaged.

* Sum arrays' elements
```sql
drop temporary function if exists sum_arrays;
create temporary function sum_arrays
  as 'org.hive.propolis.SumArraysUDAF'
;
```
Returns a new array with each element being summed up.


### HBase:

* hbase_check_value
```sql
drop temporary function if exists hbase_check_value;
create temporary function hbase_check_value
  as 'org.hive.propolis.HBaseCheckExistsUDF'
;
```
Simple Hbase key existence checker; return true if the key is in table.

* hbase_get_family
```sql
drop temporary function if exists hbase_get_family;
create temporary function hbase_get_family
  as 'org.hive.propolis.HBaseGetFamilyUDF'
;
```
Simple Hbase value getter; returns the whole family as Map for a given key.

* hbase_get_value
```sql
drop temporary function if exists hbase_get_value;
create temporary function hbase_get_value
  as 'org.hive.propolis.HBaseGetValueUDF'
;
```
Simple value getter.

* hbase_get_versions
```sql
drop temporary function if exists hbase_get_versions;
create temporary function hbase_get_versions
  as 'org.hive.propolis.HBaseGetVersionsUDF'
;
```
Simple Hbase value getter; return an array of all value version for a given key (the first array element corresponds to the latest value version).
