import elixir
import time
import schema

from type_map import type_map

import roslib
import roslib.message
roslib.load_manifest('ros_sql')
import rospy
import ros_sql.ros2sql as ros2sql

def msg2sql(topic_name, msg, timestamp=None, session=None):
    '''generate commands for saving topic'''
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
    inverses = []
    forwards = {}
    for relationship in result._descriptor.relationships:
        if isinstance( relationship, elixir.relationships.OneToMany ):
            inverses.append( relationship )
        elif isinstance( relationship, elixir.relationships.ManyToOne ):
            cnames = relationship.foreign_key
            assert len(cnames)==1
            cname = cnames[0]
            forwards[cname.name] = relationship
        elif isinstance( relationship, elixir.relationships.OneToOne ):
            pass
        else:
            raise NotImplementedError
    d=result.to_dict()
    top_level=isinstance(result, schema.RecordedEntity)

    results = {}
    if top_level:
        results['timestamp']=rospy.Time( d.pop('record_stamp_secs'), d.pop('record_stamp_nsecs') )
        # this is a hack to use elixir defaults. XXX TODO: use elixir metadata
        d.pop('recordedentity_id')
        d.pop('row_type')
    d.pop('id') # hack to use elixir default. XXX TODO: get primary key programatically

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
                new_msg = sql2msg(new_topic, field )['msg']
                d[field_name] = new_msg

    for key in d.keys():
        # XXX TODO use out-of-band channel to store time fields rather than name munging.
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
    MsgClass = ros2sql.get_msg_class(msg_class_name)

    if len(inverses):
        for inv in inverses:
            arr = []
            name = inv.name
            tn2 = topic_name + '.' + name

            tmp1 = getattr( result, name )
            for tmp2 in tmp1:
                value2 = sql2msg(tn2,tmp2)['msg']
                arr.append( value2 )

            assert name not in d
            d[name] = arr

    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def insert_row( topic_name, name, value ):
    name2 = topic_name + '.' + name
    klass = schema.get_class(name2)
    if isinstance(value, roslib.message.Message ):
        kwargs, atts = msg2dict( name2, value )
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
            # special case for array type
            refs = []
            for element in value:
                row = insert_row( topic_name, name, element )
                refs.append(row)
            result[name] = refs
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( topic_name, name, value )
            atts.append( (name, row) )
    return result, atts
