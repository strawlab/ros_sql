#!/usr/bin/env python
import sys
import re
import importlib

import sqlalchemy

from ros_sql.type_map import type_map

import roslib
roslib.load_manifest('ros_sql')
import rospy

def get_topics( topics_cli ):
    if topics_cli:
        return topics_cli
    else:
        n = rospy.get_name()+"/topics"
        return [t.replace(n,"") for t in rospy.get_param_names() if t.startswith(n)]

def get_msg_child_table(session, table_name, child_name):
    #sqlite table names cannot contain dots
    if "sqlite" in session.bind.dialect.name:
        if child_name in ("id","parent_id"):
            return table_name + "." + child_name
        else:
            return table_name + "__" + child_name
    else:
        return table_name + "." + child_name

def get_bind_url( bind_url_cli ):
    bind_url_default = 'sqlite:///:memory:'
    # command-line overrides default and rosparam
    if bind_url_cli is not None:
        bind_url = bind_url_cli
    else:
        bind_url = rospy.get_param('~bind_url',bind_url_default)
    return bind_url

def get_prefix( prefix_cli ):
    return rospy.get_param('~prefix',prefix_cli)

def get_msg_class(msg_name):
    p1,p2 = msg_name.split('/')
    module_name = p1+'.msg'
    class_name = p2
    module = importlib.import_module(module_name)
    klass = getattr(module,class_name)
    return klass

def namify( topic_name ):
    if topic_name.startswith('/'):
        topic_name = topic_name[1:]

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
