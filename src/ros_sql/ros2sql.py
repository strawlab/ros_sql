#!/usr/bin/env python
import sys
import re
from collections import OrderedDict

from type_map import type_map

import roslib
roslib.load_manifest('rosmsg')
import rosmsg
import rospy

INHERITANCE_TYPE = 'multi'
RELATIONSHIPS = "ondelete='cascade'"

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
    elif mode=='field':
        output = topic_name.lower()
    return output

def parse_field( topic_name, _type, source_topic_name, field_name ):
    """for a given element within a message, find the schema field type"""
    dt = type_map.get(_type,None)
    if dt is not None:
        # simple type
        results = {'assign':'Field(%s)'%(dt,)}
        return results

    my_class_name = namify( source_topic_name, mode='class')
    my_instance_name = namify( source_topic_name, mode='instance')
    other_class_name = namify( topic_name, mode='class')
    other_instance_name = field_name

    if _type.endswith('[]'):
        # array - need to start another sql table
        element_type = _type[:-2]
        dt = type_map.get(element_type,None)
        if dt is not None:
            # array of fundamental type
            rx = generate_schema_text(
                topic_name, dt, top=False,
                known_sql_type=True,
                relationships=[(my_instance_name,
                                'ManyToOne(%r,inverse=%r,%s)'%(
                my_class_name,other_instance_name,RELATIONSHIPS))] )

        else:
            # array of non-fundamental type
            msg_class = roslib.message.get_message_class(element_type)

            rx = generate_schema_text(
                topic_name, msg_class, top=False,
                relationships=[(my_instance_name,
                                'ManyToOne(%r,inverse=%r,%s)'%(
                my_class_name,other_instance_name,RELATIONSHIPS))] )
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

        rx = generate_schema_text(
            topic_name, msg_class, top=False,
            relationships=[(my_instance_name,
                            'OneToOne(%r,inverse=%r)'%(
            my_class_name,other_instance_name))] )
        results = {'assign':'ManyToOne(%r,inverse=%r,%s)'%(
            other_class_name,my_instance_name,RELATIONSHIPS),
                   'text':rx['schema_text'],
                   'classes':rx['class_names'],
                   'tables':rx['table_names'],
                   'topics2class_names':rx['topics2class_names'],
                   'topics2msg':rx['topics2msg'],
                   }
    return results

def dict_to_kwarg_string(kwargs):
    result = []
    for k in kwargs:
        result.append( '%s=%r'%(k, kwargs[k]))
    return ','.join(result)

def generate_schema_text( topic_name, msg_class, relationships=None, top=True,
                          known_sql_type=False):
    """convert a message type into a Python source code string"""
    class_name = namify( topic_name, mode='class')
    table_name = namify( topic_name, mode='table')

    classes = [class_name]
    tables = [table_name]
    topics2class_names = OrderedDict({topic_name: class_name})
    topics2msg = OrderedDict({topic_name: msg_class._type})
    more_texts = []

    buf = ''
    if top:
        buf += '#--------------- from topic %s\n'%topic_name
        base_name = 'RecordedEntity'
    else:
        base_name = 'Entity'
    buf += 'class %s(%s):\n'%(class_name,base_name)
    buf += "    '''schema for topic %s of type %s'''\n"%(topic_name,msg_class._type)
    opts = dict(tablename=table_name)
    if top:
        opts['inheritance']=INHERITANCE_TYPE
    buf += '    using_options(%s)\n'%dict_to_kwarg_string(opts)
    if relationships is not None:
        for name, val in relationships:
            buf += '    %s = %s\n'%(name,val)
    buf += '\n'
    if known_sql_type:
        buf += '    %s = Field(%s)\n'%(namify(topic_name,mode='field')+'_data',msg_class)
    else:
        for name, _type in zip(msg_class.__slots__, msg_class._slot_types):
            if _type=='time':
                # special type - doesn't map to 2 columns
                buf += '    %s_secs = Field(%s)  # time, ROS field: %s\n'%(name,type_map['uint64'],name)
                buf += '    %s_nsecs = Field(%s) # time, ROS field: %s\n'%(name,type_map['uint64'],name)
            else:
                results = parse_field( topic_name+'.'+name, _type, topic_name, name )
                buf += '    %s = %s\n'%(name,results['assign'])
                if 'text' in results:
                    more_texts.append(results['text'])
                    classes.extend(results['classes'])
                    tables.extend( results['tables'] )
                    print "for ",topic_name+'.'+name
                    print "results['topics2class_names']: %r"%results['topics2class_names']
                    topics2class_names.update( results['topics2class_names'] )
                    topics2msg.update( results['topics2msg'] )
    more_texts.insert(0, buf)
    final = '\n'.join(more_texts)
    return {'class_name':class_name,
            'table_names':tables,
            'class_names':classes,
            'schema_text':final,
            'topics2class_names':topics2class_names,
            'topics2msg':topics2msg,
            }

def _write_schemas( text, topics2class_names, topics2msg, all = None ):
    result = """from elixir import *

"""
    if all is not None:
        result += '__all__ = %r\n\n'%all

    result += \
"""#--------------- base class
class RecordedEntity(Entity):
    '''base class for all recorded messages (holds timestamps)'''
    using_options(tablename='recorded_entity_base',inheritance=%r)

    record_stamp_secs = Field(%s)  # time
    record_stamp_nsecs = Field(%s) # time

"""%(INHERITANCE_TYPE,type_map['uint64'],type_map['uint64'])

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

def build_schemas( list_of_topics_and_messages ):
    texts = []
    class_names = []
    topics2class_names = OrderedDict()
    topics2msg = OrderedDict()
    for topic_name, msg_class in list_of_topics_and_messages:
        rx = generate_schema_text(topic_name,msg_class)
        texts.append( rx['schema_text'] )
        class_names.extend(rx['class_names'])
        topics2class_names.update( rx['topics2class_names'] )
        topics2msg.update( rx['topics2msg'] )

    text = '\n'.join(texts)
    final_text = _write_schemas( text, topics2class_names, topics2msg, all=class_names )
    return {'schema_text':final_text,
            'topic2class':topics2class_names,
            'topic2msg':topics2msg,
            }
