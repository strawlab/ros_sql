import elixir
import time
import importlib
import schema

from type_map import type_map

import roslib; roslib.load_manifest('rospy'); import rospy

def msg2sql(topic_name, msg,timestamp=None,session=None):
    '''generate commands for saving topic /test_pose'''
    if timestamp is None:
        timestamp=rospy.Time.from_sec( time.time() )
    kwargs, atts = msg2dict(topic_name,msg)
    kwargs['record_stamp_secs']=timestamp.secs
    kwargs['record_stamp_nsecs']=timestamp.nsecs
    for name,value in atts:
        kwargs[name]=value
    klass = schema.get_class(topic_name)
    row = klass(**kwargs)
    if session is not None:
        session.add(row)
        session.commit()

def sql2msg(topic_name,result):
    '''convert query result into message'''
    print "query result: %r"%result
    print dir(result)
    d=result.to_dict()
    print d
    top_level=False
    if 'recordedentity_id' in d:
        top_level=True
        d.pop('recordedentity_id')
    results = {}
    if top_level:
        results['timestamp']=rospy.Time( d.pop('record_stamp_secs'), d.pop('record_stamp_nsecs') )
        d.pop('row_type')
    d.pop('id')

    for key in d.keys():
        if key.endswith('_id'):  # this is a hack. how to dereference properly?
            print 'key',key
            field_name = key[:-3] # this is a hack. how to dereference properly?
            print 'field_name',field_name
            if not hasattr(result,field_name): # a hack on a hack
                # no, this isn't a reference to another table
                continue
            field = getattr(result,field_name)
            print 'field',field
            new_topic = topic_name + '.' + field_name
            print 'new_topic',new_topic
            new_msg = sql2msg(new_topic, field )['msg']
            print 'new_msg',new_msg
            d.pop(key)
            d[field_name] = new_msg
            # print 'fk',fk

    for key in d.keys():
        if key.endswith('_secs'):
            # hack to detect encoded time: 2 fields with same name execpt ending
            name = key[:-5]

            name_sec = name + '_secs'
            name_nsec = name + '_nsecs'
            if not name_nsec in d:
                continue
            time_sec = d.pop(name_sec)
            time_nsec = d.pop(name_nsec)
            value = rospy.Time(time_sec,time_nsec)
            assert name not in d
            d[name] = value

    msg_class_name = schema.get_msg_name(topic_name)
    MsgClass = get_msg_class(msg_class_name)
    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def insert_row( topic_name, name, value ):
    name2 = topic_name + '.' + name
    klass = schema.get_class(name2)

    kwargs, atts = msg2dict( name2, value )
    row = klass(**kwargs)
    # recursivley do atts
    if len(atts):
        raise NotImplementedError
    return row

def msg2dict(topic_name,msg):
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
            # array type
            raise NotImplementedError
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( topic_name, name, value )
            atts.append( (name, row) )
    return result, atts

def get_msg_class(msg_name):
    p1,p2 = msg_name.split('/')
    module_name = p1+'.msg'
    class_name = p2
    module = importlib.import_module(module_name)
    klass = getattr(module,class_name)
    return klass
