#!/usr/bin/env python
import sys
import re
from collections import OrderedDict
import importlib

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.types

from type_map import type_map

import roslib
roslib.load_manifest('ros_sql')
import rosmsg
import rospy
import std_msgs

RELATIONSHIPS = "ondelete='cascade'"
ROS_SQL_COLNAME_PREFIX = '_ros_sql'

Base = sqlalchemy.ext.declarative.declarative_base()
#Base = sqlalchemy.ext.declarative.declarative_base(
#    cls=sqlalchemy.ext.declarative.DeclarativeReflectedBase)

class ROS2SQL(Base):
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_metadata'
    id =  sqlalchemy.Column(sqlalchemy.types.Integer, primary_key=True)
    topic_name = sqlalchemy.Column( sqlalchemy.types.String )
    table_name = sqlalchemy.Column( sqlalchemy.types.String )
    msg_class_name = sqlalchemy.Column( sqlalchemy.types.String )
    msg_md5sum = sqlalchemy.Column( sqlalchemy.types.String )
    is_top = sqlalchemy.Column( sqlalchemy.types.Boolean )

    def __init__(self, topic_name, table_name, msg_class, msg_md5,is_top):
        self.topic_name = topic_name
        self.table_name = table_name
        self.msg_class_name = msg_class
        self.msg_md5sum = msg_md5
        self.is_top = is_top

    def __repr__(self):
        return "<ROS2SQL(%r,%r,%r,%r)>"%( self.topic_name,
                                          self.table_name,
                                          self.msg_class_name,
                                          self.msg_md5sum,
                                          self.is_top)

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

def namify( topic_name, mode='class'):
    if topic_name.startswith('/'):
        topic_name = topic_name[1:]
    if '/' in topic_name:
        raise NotImplementedError

    topic_name = topic_name.replace('.','_')

    if mode=='module':
        output = topic_name.lower()
    elif mode=='class':
        output = capitalize(topic_name)
    elif mode=='table':
        output = topic_name.lower()
    elif mode=='instance':
        output = topic_name.lower()
    return output

def slot_type_to_class_name(element_type):
    # This is (obviously) a hack, but how to do it right?
    x = element_type.capitalize()
    if x.startswith('Uint'):
        x = 'UInt' + x[4:]
    return x

def parse_field( metadata, topic_name, _type, source_topic_name, field_name ):
    """for a given element within a message, find the schema field type"""
    dt = type_map.get(_type,None)
    tab_track_rows = []

    if dt is not None:
        # simple type
        results = {#'assign':dt,
                   'tab_track_rows':tab_track_rows,
                   'col_args': (),
                   'col_kwargs': {'type_':dt},
                   }
        return results

    my_class_name = namify( source_topic_name, mode='class')
    my_instance_name = namify( source_topic_name, mode='instance')
    other_class_name = namify( topic_name, mode='class')
    other_instance_name = field_name
    print 'parsing non-trivial field in topic %r, field name %r'%(topic_name, field_name)

    if _type.endswith('[]'):
        # array - need to start another sql table
        element_type = _type[:-2]
        dt = type_map.get(element_type,None)
        if dt is not None:
            78945/0
            element_class_name = slot_type_to_class_name(element_type)
            msg_class = getattr(std_msgs.msg,element_class_name)
            # array of fundamental type
            rx = generate_schema_raw(metadata,
                topic_name, msg_class, top=False,
                known_sql_type=dt,
                relationships=[(my_instance_name,
                                'ManyToOne(%r,inverse=%r,%s)'%(
                my_class_name,other_instance_name,RELATIONSHIPS))] )

        else:
            15043854/0
            # array of non-fundamental type
            msg_class = roslib.message.get_message_class(element_type)

            rx = generate_schema_raw(metadata,
                topic_name, msg_class, top=False,
                relationships=[(my_instance_name,
                                'ManyToOne(%r,inverse=%r,%s)'%(
                my_class_name,other_instance_name,RELATIONSHIPS))] )
        87492/0
        results = {'assign':'OneToMany(%r,inverse=%r)'%(
            other_class_name,my_instance_name),
                   'text':rx['schema_text'],
                   'classes':rx['class_names'],
                   'tables':rx['table_names'],
                   'topics2class_names':rx['topics2class_names'],
                   'topics2msg':rx['topics2msg'],
                   }
    else:
        # _type is another message type
        msg_class = roslib.message.get_message_class(_type)

        rx = generate_schema_raw(metadata,topic_name,msg_class, top=False)
        tab_track_rows.extend( rx['tracking_table_rows'] )

        for k in rx:
            v = rx[k]
            print 'rx[%r]: %r'%(k,v)

            #relationships=[(my_instance_name,('OneToOne',my_class_name, other_instance_name))])
            #                 'OneToOne(%r,inverse=%r)'%(
            # my_class_name,other_instance_name))] )

        other_key_name = my_instance_name + '.' + rx['pk_name']

        print '::::::::::::::::::::::::::::::; other_key_name: %r'%other_key_name
        print '%r'%my_instance_name

        results = {#'assign':'ManyToOne(%r,inverse=%r,%s)'%(
            #other_class_name,my_instance_name,RELATIONSHIPS),
            'col_args': ( sqlalchemy.ForeignKey(other_key_name,ondelete='cascade'), ),
            'col_kwargs': {'type_':rx['pk_type'],
                           'nullable':False,
                           },
            'tab_track_rows':tab_track_rows,
                   # 'text':rx['schema_text'],
                   # 'classes':rx['class_names'],
                   # 'tables':rx['table_names'],
                   # 'topics2class_names':rx['topics2class_names'],
                   # 'topics2msg':rx['topics2msg'],
                   }
    return results

def dict_to_kwarg_string(kwargs):
    result = []
    for k in kwargs:
        result.append( '%s=%r'%(k, kwargs[k]))
    return ','.join(result)

def add_time_cols(this_table, prefix):
    this_table.append_column(
        sqlalchemy.Column( prefix+'_secs', type_map['uint64'] ))
    this_table.append_column(sqlalchemy.Column(
        prefix+'_nsecs', type_map['uint64'] ))

def generate_schema_raw( metadata,
                         topic_name, msg_class, relationships=None, top=True,
                         known_sql_type=None):
    """convert a message type into a Python source code string"""
    tracking_table_rows = []
    #class_name = namify( topic_name, mode='class')
    table_name = namify( topic_name, mode='table')

    #classes = [class_name]
    #tables = [table_name]
    #topics2class_names = OrderedDict({topic_name: class_name})
    topics2msg = OrderedDict({topic_name: msg_class._type})
    more_texts = []

    print '                                               Table(%r)'%table_name
    this_table = sqlalchemy.Table( table_name, metadata )

    pk_name = ROS_SQL_COLNAME_PREFIX+'_id'

    assert pk_name not in msg_class.__slots__
    assert (ROS_SQL_COLNAME_PREFIX+'_timestamp_secs') not in msg_class.__slots__
    assert (ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs') not in msg_class.__slots__

    pk_type = sqlalchemy.types.Integer

    this_table.append_column(
        sqlalchemy.Column(ROS_SQL_COLNAME_PREFIX+'_id',
                          pk_type,
                          primary_key=True))

    if top:
        add_time_cols( this_table, ROS_SQL_COLNAME_PREFIX+'_timestamp' )

    # '''schema for topic %s of type %s'''%(topic_name,msg_class._type)

    if relationships is not None:
        for name, val in relationships:
            rel_type, r1, r2 = val
            print 'topic_name,rel_type,r1,r2',topic_name,rel_type,r1,r2
            1/0
            #buf += '    %s = %s\n'%(name,val)

    if known_sql_type is not None:
        this_table.append_column(sqlalchemy.Column('data', known_sql_type))
    else:
        for name, _type in zip(msg_class.__slots__, msg_class._slot_types):
            if _type=='time':
                # special type - doesn't map to 2 columns
                add_time_cols( this_table, name )
            else:
                results = parse_field( metadata, topic_name+'.'+name, _type, topic_name, name )
                tracking_table_rows.extend( results['tab_track_rows'] )
                print '--------- parse field results: %r'%results

                this_table.append_column( sqlalchemy.Column(name, *results['col_args'], **results['col_kwargs'] ))

                # assert 'text' not in results
                # if 'text' in results:

                #     # more_texts.append(results['text'])
                #     # #classes.extend(results['classes'])
                #     # #tables.extend( results['tables'] )
                #     # #topics2class_names.update( results['topics2class_names'] )
                #     # topics2msg.update( results['topics2msg'] )

    topics2tables = {topic_name : this_table}
    #topics2classes = {topic_name : this_class}
    #more_texts.insert(0, buf)
    #final = '\n'.join(more_texts)

    # args to ROS2SQL()
    tracking_table_rows.append(  (topic_name, table_name, msg_class._type, msg_class._md5sum, top) )

    return {#'topics2tables':topics2tables,
            #'topics2classes':topics2classes,
            #'topics2msg':topics2msg,

            'tracking_table_rows':tracking_table_rows,
            'pk_type':pk_type,
            'pk_name':pk_name,
            }

def _write_schemas( text, topics2class_names, topics2msg, all = None ):
    19043239/0
    result = """from elixir import *

"""
    if all is not None:
        result += '__all__ = %r\n\n'%all

    result += text

    result += \
"""
#--------------- helper functions
def get_class( topic_name ):
    '''get the schema for topic_name'''
    d = {
"""
    for key in topics2class_names:
        result += '        %r:%s,\n'%(key,topics2class_names[key])
    result += '    }\n'
    result += '    return d[topic_name]\n'

    result += """
def get_msg_name( topic_name ):
    '''get the msg name for topic_name'''
    d = {
"""
    for key in topics2msg:
        result += '        %r:%r,\n'%(key,topics2msg[key])
    result += '    }\n'
    result += '    return d[topic_name]\n'

    result += """
def have_topic( topic_name ):
    d = %r
"""%(topics2msg.keys(),)
    result += '    return (topic_name in d)\n'

    return result

def gen_schema( metadata, topic_name, msg_class, top=True):
    # add table(s) to MetaData instance
    rx = generate_schema_raw(metadata,topic_name,msg_class, top=top)

    # add table tracking row to MetaData instance
    Base.metadata.reflect( metadata.bind )
    Base.metadata.create_all( metadata.bind )
    Session = sqlalchemy.orm.sessionmaker(bind=metadata.bind)
    session = Session()

    for new_meta_row_args in rx['tracking_table_rows']:
        new_meta_row = ROS2SQL(*new_meta_row_args)
        session.add(new_meta_row)
    session.commit()
    #return rx

def add_schemas( metadata, list_of_topics_and_messages ):
    for topic_name, msg_class in list_of_topics_and_messages:
        gen_schema( metadata, topic_name, msg_class )
