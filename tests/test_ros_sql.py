import elixir
import sqlalchemy

import roslib
roslib.load_manifest('ros_sql')
import ros_sql.ros2sql as ros2sql

import std_msgs.msg

roslib.load_manifest('geometry_msgs')
import geometry_msgs.msg

class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

def setup():
    # import rospy
    # rospy.init_node('test_ros_sql',anonymous=True)
    reset_sqlalchemy()
def reset_sqlalchemy():
    if 0:
        # reset all elixir state information
        elixir.session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker())
        elixir.metadata = sqlalchemy.MetaData()
        elixir.metadatas = set()
        elixir.entities = elixir.GlobalEntityCollection()

    # connect to in-memory db
    elixir.metadata.bind = "sqlite:///:memory:"
    elixir.metadata.bind.echo = True

def test_simple_message_roundtrip():
    pose1 = geometry_msgs.msg.Pose()
    pose1.position.x = 1
    pose1.position.y = 2
    pose1.position.z = 3

    pose1.orientation.x = 0
    pose1.orientation.y = 0
    pose1.orientation.z = 0
    pose1.orientation.w = 1

    pa1 = geometry_msgs.msg.PoseArray()
    pa1.header.seq = 938
    pa1.header.stamp.secs = 3928
    pa1.header.stamp.nsecs = 9095
    pa1.header.frame_id = 'my frame'
    pa1.poses.append( pose1 )

    for tn,mc,md in [('/test_string',std_msgs.msg.String, std_msgs.msg.String('xyz')),
                     #('/test_int8', std_msgs.msg.Int8, std_msgs.msg.Int8(-4)),
                     #('/test_uint8', std_msgs.msg.UInt8, std_msgs.msg.UInt8(254)),
                     #('/test_pose', geometry_msgs.msg.Pose, pose1),
                     #('/test_pose_array', geometry_msgs.msg.PoseArray, pa1),
                     ]:
        yield check_roundtrip, tn,mc,md

def check_roundtrip( topic_name, msg_class, msg_expected ):
    #reset_sqlalchemy()

    schema_results = ros2sql.build_schemas([(topic_name,msg_class)])
    if 1:
        with open('schema.py',mode='w') as fd:
            fd.write(schema_results['schema_text'])

    USE_EXEC=False
    if USE_EXEC:
        schema_ns = {}
        exec schema_results['schema_text'] in schema_ns
        schema = Bunch(**schema_ns) # make it act like a module
        del schema_ns
    else:
        import schema

    elixir.setup_all()
    elixir.create_all()
    print '*'*100,'done creating schema'

    fr = ros2sql.build_factories([(topic_name,msg_class)],
                                 'schema',
                                 schema_results['topic2class'],
                                 )
    if 1:
        with open('factories.py',mode='w') as fd:
            fd.write(fr['factory_text'])

    if USE_EXEC:
        factory_ns = {}
        exec fr['factory_text'] in factory_ns
        factories = Bunch(**factory_ns)
        del factory_ns
    else:
        import factories

    factory = factories.get_factory(topic_name)
    print '*'*100,'calling factory'
    factory(msg_expected,session=elixir.session)

    print '*'*100,'should have made row by now'

    class_ = schema.get_class(topic_name)
    msg_actual_sql = class_.query.one()

    reverse_factory = factories.get_reverse_factory(topic_name)
    timestamp, msg_actual = reverse_factory( msg_actual_sql )

    print
    print 'msg_actual'
    print msg_actual
    print 'msg_expected'
    print msg_expected
    assert msg_actual == msg_expected
