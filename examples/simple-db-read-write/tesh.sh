#!/bin/sh

rostopic pub /nice std_msgs/String indeed -r 100 __name:=pub1 &
rostopic pub /test std_msgs/Int32 5 -r 10 __name:=pub2 &

rm -f dbname

rosrun ros_sql ros_sql record --bind sqlite:///dbname --all --prefix foo __name:=reca &
sleep 5
rosnode kill reca

sleep 2

#rosrun ros_sql ros_sql record --bind sqlite:///dbname --all --prefix bar __name:=recb &
#sleep 5
#rosnode kill recb

rosnode kill pub1
rosnode kill pub2
sleep 1
