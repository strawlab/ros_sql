"""given a pre-existing database schema, convert ROS messages back and forth"""
import time
from type_map import type_map
import sqlalchemy

import roslib
import roslib.message
roslib.load_manifest('ros_sql')
import rospy
import ros_sql.ros2sql as ros2sql

ROS_SQL_COLNAME_PREFIX = ros2sql.ROS_SQL_COLNAME_PREFIX

def get_sql_table( metadata, topic_name ):
    table_name = ros2sql.namify( topic_name, mode='table')
    return metadata.tables[table_name]

def msg2sql(metadata, topic_name, msg, timestamp=None):
    '''generate commands for saving topic'''
    if timestamp is None:
        timestamp=rospy.Time.from_sec( time.time() )
    kwargs, atts = msg2dict(metadata,topic_name,msg)
    kwargs[ROS_SQL_COLNAME_PREFIX+'_timestamp_secs']=timestamp.secs
    kwargs[ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs']=timestamp.nsecs
    for name,value in atts:
        kwargs[name]=value

    this_table = get_sql_table( metadata, topic_name )
    ins = this_table.insert()

    engine = metadata.bind
    conn = engine.connect()
    trans = conn.begin()
    try:
        result = conn.execute(ins,[kwargs])
        trans.commit()
    except:
        trans.rollback()
        raise

def get_table_info(topic_name,metadata):
    Session = sqlalchemy.orm.sessionmaker(bind=metadata.bind)
    session = Session()
    mymeta=session.query(ros2sql.ROS2SQL).filter_by(topic_name=topic_name).one()
    print 'mymeta: %r'%mymeta.msg_class_name
    #msg_class_name = schema.get_msg_name(topic_name)
    MsgClass = ros2sql.get_msg_class(mymeta.msg_class_name)
    return {'class':MsgClass,
            'top':mymeta.is_top,
            }

def sql2msg(topic_name,result,metadata):
    '''convert query result into message'''
    info = get_table_info( topic_name, metadata )
    MsgClass = info['class']

    inverses = []
    forwards = {}
    # for relationship in result._descriptor.relationships:
    #     if isinstance( relationship, elixir.relationships.OneToMany ):
    #         inverses.append( relationship )
    #     elif isinstance( relationship, elixir.relationships.ManyToOne ):
    #         cnames = relationship.foreign_key
    #         assert len(cnames)==1
    #         cname = cnames[0]
    #         forwards[cname.name] = relationship
    #     elif isinstance( relationship, elixir.relationships.OneToOne ):
    #         pass
    #     else:
    #         raise NotImplementedError
    print '='*100
    print 'result: %r'%type(result)
    print 'result: %r'%result
    print
    #d=result.to_dict()
    d = dict(result)

    results = {}
    if info['top']:
        # It is a top-level table.
        top_secs = d.pop(ROS_SQL_COLNAME_PREFIX+'_timestamp_secs')
        top_nsecs = d.pop(ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs')
        results['timestamp'] = rospy.Time( top_secs, top_nsecs )

    d.pop(ROS_SQL_COLNAME_PREFIX+'_id')

    for key in d.keys():
        if key in forwards:
            relationship = forwards[key]
            field_name = relationship.name
            field = getattr(result,field_name)
            new_topic = topic_name + '.' + field_name
            d.pop(key)
            if schema.have_topic(new_topic):
                # This is a semi-hack to restrict us from going back
                # into relationships we already went forward on.
                new_msg = sql2msg(new_topic, field, metadata )['msg']
                d[field_name] = new_msg

    if len(inverses):
        for inv in inverses:
            arr = []
            name = inv.name
            tn2 = topic_name + '.' + name

            tmp1 = getattr( result, name )
            for tmp2 in tmp1:
                value2 = sql2msg(tn2,tmp2,metadata)['msg']
                arr.append( value2 )

            assert name not in d
            d[name] = arr
    print 'd: %r'%d
    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def insert_row( metadata, topic_name, name, value ):
    name2 = topic_name + '.' + name
    klass = get_table_info( name2, metadata )['class']

    if isinstance(value, roslib.message.Message ):
        kwargs, atts = msg2dict( metadata, name2, value )
        kw2 = {}
        for k,v in atts:
            assert k not in kwargs # not already there
            assert k not in kw2 # not overwriting
            kw2[k]=v
        kwargs.update(kw2)
    else:
        kwargs = {'data':value}
    row = klass(**kwargs)
    return row

def msg2dict(metadata,topic_name,msg):
    result = {}
    atts = []
    for name,_type in zip(msg.__slots__, msg._slot_types):
        value = getattr(msg,name)
        if _type in type_map:
            # simple type
            result[name] = value
        elif _type=='time':
            # special case for time type
            result[name+'_secs'] = value.secs
            result[name+'_nsecs'] = value.nsecs
        elif _type.endswith('[]'):
            # special case for array type
            refs = []
            for element in value:
                row = insert_row( metadata, topic_name, name, element )
                refs.append(row)
            result[name] = refs
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( metadata, topic_name, name, value )
            atts.append( (name, row) )
    return result, atts
