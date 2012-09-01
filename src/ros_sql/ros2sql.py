#!/usr/bin/env python
import sys
import re
from collections import OrderedDict

import roslib
roslib.load_manifest('rosmsg')
import rosmsg
import rospy

INHERITANCE_TYPE = 'multi'
RELATIONSHIPS = "onupdate='cascade',ondelete='cascade'"

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

type_map = {
    'int8':
        'SmallInteger()',
    'uint8':
        'SmallInteger(unsigned=True)',
    'byte':
        'SmallInteger(unsigned=True)',
    'int16':
        'Integer()',
    'uint16':
        'Integer(unsigned=True)',
    'int32':
        'Integer()',
    'uint32':
        'Integer(unsigned=True)',
    'int64':
        'BigInteger()',
    'uint64':
        'BigInteger(unsigned=True)',
    'float32':
        'Float(precision=32)',
    'float64':
        'Float(precision=64)',
    'string':
        'String()',
    }

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
        results = {'assign':'OneToMany(%r,inverse=%r,%s)'%(
            other_class_name,my_instance_name,RELATIONSHIPS),
                   'text':rx['schema_text'],
                   'classes':rx['class_names'],
                   'tables':rx['table_names'],
                   'topics2class_names':rx['topics2class_names'],
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
                   }
    return results

def dict_to_kwarg_string(kwargs):
    result = []
    for k in kwargs:
        result.append( '%s=%r'%(k, kwargs[k]))
    return ','.join(result)

def generate_factory_text( topic_name, msg_class, schema_module_name,
                           topics2class_names, top=False):
    print 'building factory for %r'%topic_name
    factory_name = namify( topic_name, mode='instance')
    forward_factory_name = 'forward_%s'%factory_name
    reverse_factory_name = 'reverse_%s'%factory_name
    schema_class_name = topics2class_names[topic_name]

    buf = ''
    if top:
        buf += '#--------------- from topic %s\n'%topic_name

    if schema_module_name:
        schema_module = schema_module_name+'.'
    else:
        schema_module = ''

    buf += 'def %s(msg,timestamp=None,session=None):\n'%forward_factory_name
    buf += "    '''generate commands for saving topic %s'''\n"%topic_name
    buf += '    if timestamp is None:\n'
    # use time.time() instead of rospy.get_rostime() so we don't need ROS master
    #buf += '        timestamp=rospy.get_rostime()\n'
    buf += '        timestamp=rospy.Time.from_sec( time.time() )\n'
    buf += '    print "making row start"\n'
    buf += '    kwargs = msg2dict(msg)\n'
    buf += "    kwargs['record_stamp_secs']=timestamp.secs\n"
    buf += "    kwargs['record_stamp_nsecs']=timestamp.nsecs\n"
    buf += '    row = %s%s(**kwargs)\n'%(schema_module, schema_class_name)
    buf += '    print kwargs\n'
    buf += '    print "making row done"\n'
    buf += '    print "session %r"%session\n'
    buf += '    if session is not None:\n'
    buf += '        session.add(row)\n'
    buf += '        session.commit()\n'
    buf += '        print "commited"\n\n'


    buf += 'def %s(result):\n'%reverse_factory_name
    buf += "    '''convert query result into message'''\n"
    buf += '    print "query result: %r"%result\n'
    buf += '    print dir(result)\n'
    buf += '    d=result.to_dict()\n'
    buf += '    print d\n'
    buf += '    print result.data\n'
    buf += '    print result.data\n'
    buf += "    timestamp=rospy.Time( d.pop('record_stamp_secs'), d.pop('record_stamp_nsecs') )\n"
    buf += "    d.pop('recordedentity_id')\n"
    buf += "    d.pop('id')\n"
    buf += "    d.pop('row_type')\n"
    buf += '    MsgClass = get_msg_class(%r)\n'%msg_class._type
    buf += '    msg = MsgClass(**d)\n'
    buf += '    return timestamp, msg\n'


    topic2funcname = OrderedDict({topic_name:forward_factory_name})
    topic2revname = OrderedDict({topic_name:reverse_factory_name})

    return {'factory_text':buf,
            'topic2funcname':topic2funcname,
            'topic2revname':topic2revname,
            }

def generate_schema_text( topic_name, msg_class, relationships=None, top=True,
                          known_sql_type=False):
    """convert a message type into a Python source code string"""
    class_name = namify( topic_name, mode='class')
    table_name = namify( topic_name, mode='table')

    classes = [class_name]
    tables = [table_name]
    topics2class_names = OrderedDict({topic_name: class_name})
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
    more_texts.insert(0, buf)
    final = '\n'.join(more_texts)
    return {'class_name':class_name,
            'table_names':tables,
            'class_names':classes,
            'schema_text':final,
            'topics2class_names':topics2class_names,
            }

def _write_factories( text, topic2funcname, topic2revname, schema_module_name ):
    #result = 'from elixir import *\n'
    result = 'import elixir\n'
    result += 'import time\n'
    result += 'import importlib\n'
    result += 'import %s\n\n'%schema_module_name
    result += "import roslib; roslib.load_manifest('rospy'); import rospy\n\n"
    result += text

    topic2funcname_buf = ''
    for topic in topic2funcname:
        topic2funcname_buf += '        %r:%s,\n'%(topic,topic2funcname[topic])

    topic2revname_buf = ''
    for topic in topic2revname:
        topic2revname_buf += '        %r:%s,\n'%(topic,topic2revname[topic])

    result += "\ntype_map = %r\n"%type_map

    result += """
def msg2dict(msg):
    result = {}
    for name,_type in zip(msg.__slots__, msg._slot_types):
        if _type in type_map:
            # simple type
            result[name] = getattr(msg,name)
        else:
            raise NotImplementedError
    return result
"""
    result += """
def get_factory(topic_name):
    d = {
%s    }
    return d[topic_name]
"""%( topic2funcname_buf )

    result += """
def get_reverse_factory(topic_name):
    d = {
%s    }
    return d[topic_name]
"""%( topic2revname_buf )

    result += """
def get_msg_class(msg_name):
    p1,p2 = msg_name.split('/')
    module_name = p1+'.msg'
    class_name = p2
    module = importlib.import_module(module_name)
    klass = getattr(module,class_name)
    return klass
"""
    return result

def _write_schemas( text, topics2class_names, all = None ):
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
#--------------- helper function
def get_class( topic_name ):
    '''get the schema for topic_name'''
    d = {
"""
    for key in topics2class_names:
        result += '        %r:%s,\n'%(key,topics2class_names[key])
    result += '    }\n'
    result += '    return d[topic_name]\n'

    return result

def build_schemas( list_of_topics_and_messages ):
    texts = []
    class_names = []
    topics2class_names = OrderedDict()
    for topic_name, msg_class in list_of_topics_and_messages:
        rx = generate_schema_text(topic_name,msg_class)
        texts.append( rx['schema_text'] )
        class_names.extend(rx['class_names'])
        topics2class_names.update( rx['topics2class_names'] )

    text = '\n'.join(texts)
    final_text = _write_schemas( text, topics2class_names, all=class_names )
    return {'schema_text':final_text,
            'topic2class':topics2class_names}

def build_factories( list_of_topics_and_messages,
                     schema_module_name, topic2class ):
    texts = []
    topic2funcname = OrderedDict()
    topic2revname = OrderedDict()
    for topic_name, msg_class in list_of_topics_and_messages:
        rx = generate_factory_text(topic_name,msg_class,
                                   schema_module_name, topic2class)
        texts.append( rx['factory_text'] )
        topic2funcname.update( rx['topic2funcname'] )
        topic2revname.update( rx['topic2revname'] )

    text = '\n'.join(texts)
    final_text = _write_factories(text, topic2funcname, topic2revname,
                                  schema_module_name)
    return {'factory_text':final_text}
