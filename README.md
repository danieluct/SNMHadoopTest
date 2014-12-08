SNMHadoopTest
=============
The solution only uses basic Hadoop (API 2.2+), and was tested on a Cloudera 
extra-small cluster (1 master and 3 agents). A classic PostgreSQL database was 
installed on the master and used to store the products table. In order to make 
use of the advanced features of Hadoop not availables (as far as I could 
ascertain) to the streaming api function, I chose to provide a pure Java 
solution (which provides features such as DBInputFormat, MultipleInputs, 
PartitionerClass, GroupingComparatorClass, etc).

As the database was a bottleneck (the problem description didn't mention 
multiple instances of this db), I chose a solution that minimized database 
interaction. Therefore, database is accessed a number of times equal with the 
number of Hadoop determined mappers (+further access whenever a job fails), and 
each access gets a different chunk of the products tabel.

For simplicity I have decided to chain two Map-Reduce jobs, using temporary HDFS
files to communicate results between the two processes

--------------------------
Map-Reduce Job 1
The first Map-Reduce job includes two types of Mappers (one for reading product 
information from the database, and one for reading log files). 

The Mapper dealing with the DB does no processing beside filtering unneeded 
columns.
The Mapper dealing with logs removes superfluous date information and replaces
it with info about theQuarter (I assumed the first quarter starts on January 
1st).
Both Mappers emit a key containing ProductID and UserID. It should be noted that
the DB Mapper has no user information, and thus defaults to -1. This value was 
chosen not only because database IDs are generally positive, but also because
it allows leveraging a feature of the Hadoop shuffler. Thus, due to the way
in which defined the compare operation between keys and the GroupingComparator,
a ProductsID will be processed at only one Reducer, with the record from the 
DB Mapper always being processed before any other record for the same ProductID
comming from one of the Log Mappers. The Mapper output value is constituted by
a tuple containing a String and a Number. For the DB Mapper, these represent the
product category and price, while for the Log Mapper, these represent the 
yearly quarter and associated quantity.

The Reducer takes advantage of the peculiar sort of the keys in order to compute
a memory conservative join between the products table and the logs. The reducer
also further aggregates yearly data for each <product,user> key. It should be 
noted that a preliminary aggregation of quarters is also done, when possible, by
the combiner class associated with the Map-Reduce process. The Reducer writes
to file a string cotaining the userID, product category, quantity, and quarterly
revenue. Note that at this level the data is not aggregated at category level,
the category representing just a replacement for the product id

--------------------------
Map-Reduce Job 2
I would have liked to chain a second Reducer directly to the input of the 
previous Reducer. Unfortuntely this is not yet possible using the current Hadoop
API.
Therefore the Mapper (a single one on this occasion) is rather simple, just
extracting lines from the temporary files and preparing them for the shuffle and
sort phase. The emitted key is of <userId, category> type, while the value
contains yearly quantity and quarterly revenue.
The custom sort order, partitioner and group comparator ensure that all 
categories for a certain user are processed at a single reduce node. Furthermore,
they guarantee that once the category for a certain user has changed, none of 
the former categories will be seen again for the same user.
This characteristic provides the Reducer with an easy way to compute both the
most popular category as well as total quarterly revenues for that category with
very insignificant memory use. In order to account for two categories having the
same popularity, we allow more than one category to be expressed in the final
result. The reducers then dumps the results to a file.

-------------------------
As I considered the aggregation of the result file to be a trivial task (a 
simple Map Task emitting for each userid the rest of the values, and a single 
Reduce Task gathering the records and writing them to file), I did not further
pursue it.

Besides the Eclipse project, the solution also includes a build script for 
building the Hadoop job on Cloudera. The main class is called SNMHadoopTest.
The build.sh scripts takes no parameters and creates a jar file having the 
same name as the main class

The Job required four parameters, of which only 3 are relevant: the first and 
the latter two. The first parameter represents the HDFS path to the input files. 
FileInputFormat is able to gunzip by itself the archive, thus it didn't require 
any further work from me.
The second parameter represents a random path in the HDFS - the current 
implementation of MultipleInputs requires each of the inputs to have a separate 
path, even if one of the inputs is actually a database, as it was in my case. 
The third parameter representens a HDFS path holding the temporary results, 
while the fourth points to a HDFS location where the output files would be 
written. 
Sample run command:
 hadoop jar SNMhadooptest.jar SNMHadoopTest /user/root/sanoma/input 
 /user/root/sanoma/db /user/root/sanoma/temp 
 /user/root/sanoma/output -libjars ./lib/postgresql-8.4-703.jdbc4.jar

For simplicity I decided to hardcode the SQL connection in my Main class. In 
order for the DBMapper to be able to connect to the database, the jdbc library 
must be included in the job with the libjars option. Furthermore,the HADOOP_CLASSPATH
environment variable should be set and include the location of the jdbc library.
(At least that's the case on Cloudera).

Further notes:
I took a memory conservative approach, offloading all sorting and grouping to 
the Hadoop system. In real world scenario, some local aggregation may be desired
in order to reduce network traffic. Furthermore, depending on the available 
local storage, the database table may be read in full and made available as a
local file to each Mapper, in order to accelerate the joining, potentially
eliminating the second Map-Reduce job.
