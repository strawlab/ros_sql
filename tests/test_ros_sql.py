import sqlalchemy

import roslib
roslib.load_manifest('ros_sql')
import ros_sql.msg
import ros_sql.session
import ros_sql.ros2sql as ros2sql
import ros_sql.factories as factories

import std_msgs.msg

roslib.load_manifest('geometry_msgs')
import geometry_msgs.msg
import rospy

class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

def test_simple_message_roundtrip():
    pose1 = geometry_msgs.msg.Pose()
    pose1.position.x = 1
    pose1.position.y = 2
    pose1.position.z = 3

    pose1.orientation.x = -0.01
    pose1.orientation.y = 0
    pose1.orientation.z = 0
    pose1.orientation.w = 4.56

    pose2 = geometry_msgs.msg.Pose()
    pose2.position.x = 2
    pose2.position.y = 3
    pose2.position.z = 4

    pose2.orientation.x = -0.02
    pose2.orientation.y = 1
    pose2.orientation.z = 3
    pose2.orientation.w = 5.67

    header = std_msgs.msg.Header()
    header.seq = 938
    header.stamp.secs = 3928
    header.stamp.nsecs = 9095
    header.frame_id = 'my frame'

    pa1 = geometry_msgs.msg.PoseArray()
    pa1.header = header
    pa1.poses.append( pose1 )
    pa1.poses.append( pose2 )

    tc = ros_sql.msg.TestComplex()
    tc.pa = pa1
    tc.string_array = map( std_msgs.msg.String, [ 'here', 'are', 'some', '\0complex', 'strings' ])
    tc.uint8_array = map( std_msgs.msg.UInt8, [  0, 3, 254 ])
    tc.header = header

    bma = std_msgs.msg.ByteMultiArray()
    bma.data = [ std_msgs.msg.Byte(x) for x in [77, 33, 254] ]

    for tn,mc,md in [('/test_string',std_msgs.msg.String, std_msgs.msg.String('xyz')),
                     ('/test_int8', std_msgs.msg.Int8, std_msgs.msg.Int8(-4)),
                     ('/test_uint8', std_msgs.msg.UInt8, std_msgs.msg.UInt8(254)),
                     ('/test_pose', geometry_msgs.msg.Pose, pose1),
                     ('/myheader', std_msgs.msg.Header, header),
                     ('/byte_arr_topic', std_msgs.msg.ByteMultiArray, bma),
                     ('/test_pose_array', geometry_msgs.msg.PoseArray, pa1),
                     ('/test_complex', ros_sql.msg.TestComplex, tc),
                     ]:
        yield check_roundtrip, tn,mc,md

def check_roundtrip( topic_name, msg_class, msg_expected, strict=True ):
    engine = sqlalchemy.create_engine('sqlite:///:memory:')#, echo=True)
    metadata = sqlalchemy.MetaData(bind=engine)
    session = ros_sql.session.get_session(metadata)

    ros2sql.add_schemas(session,metadata,[(topic_name,msg_class)])
    metadata.create_all()

    if strict:
        # ensure that ROS actually accepts these data
        rospy.init_node('test_ros_sql')
        pub = rospy.Publisher( topic_name, msg_class )
        pub.publish( msg_expected )

    # send to SQL
    factories.msg2sql(session, metadata, topic_name, msg_expected)

    # get back from SQL
    this_table = factories.get_sql_table(session, metadata, topic_name)
    s = sqlalchemy.sql.select([this_table])

    conn = metadata.bind.connect()
    sa_result = conn.execute(s)
    msg_actual_sql = sa_result.fetchone()
    sa_result.close()

    result = factories.sql2msg( topic_name, msg_actual_sql, session, metadata ) # convert to ROS

    timestamp = result['timestamp']
    msg_actual = result['msg']

    # print 'FINAL *************'
    # print
    # print 'msg_expected'
    # print msg_expected
    # print
    # print 'msg_actual'
    # print msg_actual
    assert msg_actual == msg_expected
