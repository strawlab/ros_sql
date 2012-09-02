ros_sql
=======

Log arbitrary ROS messages to an SQL database.

TODO
----

1) Remove dependency on elixir to have DB layer as pure sqlalchemy.
2) Generate schema dynamically.  (Currently, we generate python source
   code, which then must be imported or execed, causing other
   problems.)
