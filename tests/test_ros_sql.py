import sqlalchemy
import StringIO

import roslib
roslib.load_manifest('ros_sql')
import rospy

import ros_sql.msg
import ros_sql.session
import ros_sql.ros2sql as ros2sql
import ros_sql.factories as factories

import std_msgs.msg

roslib.load_manifest('geometry_msgs')
import geometry_msgs.msg

class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

_args = {}
def set_args(*args):
    global _args
    key = args[0]
    if key in _args:
        raise ValueError('%r already in _args'%key)
    _args[key] = args[1:]

def get_args(key):
    global _args
    return _args[key]

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
    tc.string_array = [ 'here', 'are', 'some', '\0complex', 'strings' ]
    tc.uint8_array = [  0, 3, 254 ]
    tc.header = header

    bma = std_msgs.msg.ByteMultiArray()
    bma.data = [77, 33, -127]

    for tn,mc,md in [('/test_string',std_msgs.msg.String, std_msgs.msg.String('xyz')),
                     ('/test_string2',std_msgs.msg.String, std_msgs.msg.String('x\0yz')),
                     ('/test_bool', std_msgs.msg.Bool, std_msgs.msg.Bool(True)),
                     ('/test_char', std_msgs.msg.Char, std_msgs.msg.Char(254)),
                     ('/test_duration', std_msgs.msg.Duration, std_msgs.msg.Duration(rospy.Duration(1,2))),
                     ('/test_time', std_msgs.msg.Time, std_msgs.msg.Time(rospy.Time(1,2))),
                     ('/test_empty', std_msgs.msg.Empty, std_msgs.msg.Empty()),
                     ('/test_int8', std_msgs.msg.Int8, std_msgs.msg.Int8(-4)),
                     ('/test_uint8', std_msgs.msg.UInt8, std_msgs.msg.UInt8(254)),
                     ('/test_int16', std_msgs.msg.Int16, std_msgs.msg.Int16(-190)),
                     ('/test_uint16', std_msgs.msg.UInt16, std_msgs.msg.UInt16(2**15+1)),
                     ('/test_int32', std_msgs.msg.Int32, std_msgs.msg.Int32(-190)),
                     ('/test_uint32', std_msgs.msg.UInt32, std_msgs.msg.UInt32(2**31+1)),
                     ('/test_int64', std_msgs.msg.Int64, std_msgs.msg.Int64(-190)),
                     ('/test_uint64', std_msgs.msg.UInt64, std_msgs.msg.UInt64(2**62+1)),
                     ('/test_pose', geometry_msgs.msg.Pose, pose1),
                     ('/myheader', std_msgs.msg.Header, header),
                     ('/byte_arr_topic', std_msgs.msg.ByteMultiArray, bma),
                     ('/test_pose_array', geometry_msgs.msg.PoseArray, pa1),
                     ('/test_complex', ros_sql.msg.TestComplex, tc),
                     ]:
        set_args(tn, mc, md) # workaround nose bug when dealing with unicode
        yield check_roundtrip, tn

def check_roundtrip( topic_name, strict=True ):
    msg_class, msg_expected = get_args( topic_name ) # workaround

    engine = sqlalchemy.create_engine('sqlite:///:memory:')#, echo=True)
    metadata = sqlalchemy.MetaData(bind=engine)
    session = ros_sql.session.get_session(metadata)

    ros2sql.add_schemas(session,metadata,[(topic_name,msg_class)])
    metadata.create_all()

    if strict:
        # ensure that ROS actually accepts these data
        buf = StringIO.StringIO()
        msg_expected.serialize( buf )

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
