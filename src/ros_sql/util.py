#!/usr/bin/env python
import sys
import re
import importlib

import sqlalchemy

from ros_sql.type_map import type_map

import roslib
roslib.load_manifest('ros_sql')
import rospy

def get_msg_class(msg_name):
    p1,p2 = msg_name.split('/')
    module_name = p1+'.msg'
    class_name = p2
    module = importlib.import_module(module_name)
    klass = getattr(module,class_name)
    return klass

def capitalize(name):
    "name_with_parts -> NameWithParts"
    parts = name.split('_')
    parts = [p.capitalize() for p in parts]
    return ''.join(parts)

def namify( topic_name ):
    if topic_name.startswith('/'):
        topic_name = topic_name[1:]
    if '/' in topic_name:
        raise NotImplementedError

    topic_name = topic_name.replace('.','_')

    return topic_name.lower()

def slot_type_to_class_name(element_type):
    # This is (obviously) a hack, but how to do it right?
    x = element_type.capitalize()
    if x.startswith('Uint'):
        x = 'UInt' + x[4:]
    return x

def time_cols_to_ros( time_secs, time_nsecs, is_duration=False ):
    time_secs2 = int(time_secs)
    time_nsecs2 = int(time_nsecs)
    if time_secs2 != time_secs:
        raise ValueError('time value (%r) cannot be represented as integer.'%time_secs)
    if time_nsecs2 != time_nsecs:
        raise ValueError('time value (%r) cannot be represented as integer.'%time_nsecs)
    if not is_duration:
        return rospy.Time( time_secs2, time_nsecs2 )
    else:
        return rospy.Duration( time_secs2, time_nsecs2 )

def add_time_cols(this_table, prefix, duration=False):
    c1 = sqlalchemy.Column( prefix+'_secs',  type_map['uint64'] )
    c2 = sqlalchemy.Column( prefix+'_nsecs', type_map['uint64'] )
    this_table.append_column(c1)
    this_table.append_column(c2)
    if not duration:
        ix = sqlalchemy.schema.Index( 'ix_'+this_table.name+prefix, c1, c2 )
        return ix
    else:
        return None
