*******
ros_sql
*******

Log arbitrary ROS messages to an SQL database.

ros_sql a ROS-specific ORM which:

* transforms ROS message types to SQL database schemas
* saves ROS messages to such a database (e.g. like rosbag record)
* creates ROS messages from such a database (e.g. like rosbag play)

Logging ROS messages to an SQL database
=======================================

`rosrun ros_sql record` is much like `rosbag record` -- you can give a
list of topic names to record or specify `--all` (or `-a`) to record
all topics.

Does ros_sql store the ROS message defintion in the SQL database?
-----------------------------------------------------------------

No, the ROS message definition is not stored in the database. If your
ROS installation changes the definition of a message type, ros_sql
will raise an error because the md5sum of the message definition has
changed. However, the data is all still in the database with a fairly
natural conversion.

What happens if I upgrade ros_sql - will I lose my database?
------------------------------------------------------------

All metadata specific to ros_sql is saved with a schema version
number, so it should be possible to migrate your database to support a
newer version of ros_sql. A runtime check ensures that the current
database is using the expected schema version.

Do you also reflect database contents to Python classes?
--------------------------------------------------------

The database contents are reflected back and forth from ROS
messages. No other representation is supported.

That said, it is straightforward to use sqlalchemy to reflect the
database into Python objects. This could facilitate querying the
database and perhaps some examples doing this will be added in the
future.
