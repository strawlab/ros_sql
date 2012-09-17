#!/usr/bin/env python
import sys
import re
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

ROS_SQL_COLNAME_PREFIX = '_ros_sql'
SCHEMA_VERSION = 1

Base = sqlalchemy.ext.declarative.declarative_base()

class RosSqlMetadataBackrefs(Base):
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_backref_metadata'
    id =  sqlalchemy.Column(sqlalchemy.types.Integer, primary_key=True)
    parent_field = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)
    child_table = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)
    child_field = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)

    main_table_name = sqlalchemy.Column( sqlalchemy.types.String,
                                         sqlalchemy.ForeignKey(ROS_SQL_COLNAME_PREFIX + '_metadata.table_name' ),
                                         nullable=False)
    def __init__(self, parent_field, child_table, child_field):
        self.parent_field = parent_field
        self.child_table = child_table
        self.child_field = child_field

    def __repr__(self):
        return '<RosSqlMetadataBackrefs(%r,%r,%r)'%(
            self.parent_field, self.child_table, self.child_field)

class RosSqlMetadataTimestamps(Base):
    """keep track of names of Time fields"""
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_timestamp_metadata'
    id =  sqlalchemy.Column(sqlalchemy.types.Integer, primary_key=True)
    column_base_name = sqlalchemy.Column( sqlalchemy.types.String,
                                          nullable=False )

    main_table_name = sqlalchemy.Column( sqlalchemy.types.String,
                                         sqlalchemy.ForeignKey(ROS_SQL_COLNAME_PREFIX + '_metadata.table_name' ),
                                         nullable=False)

    def __init__(self, column_base_name):
        self.column_base_name = column_base_name
    def __repr__(self):
        return '<RosSqlMetadataTimestamps(%r)>'%(self.column_base_name)

class RosSqlMetadata(Base):
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_metadata'
    topic_name = sqlalchemy.Column( sqlalchemy.types.String, primary_key=True)
    table_name = sqlalchemy.Column( sqlalchemy.types.String, primary_key=True)
    msg_class_name = sqlalchemy.Column( sqlalchemy.types.String, nullable=False )
    msg_md5sum = sqlalchemy.Column( sqlalchemy.types.String, nullable=False )
    is_top = sqlalchemy.Column( sqlalchemy.types.Boolean, nullable=False )
    pk_name = sqlalchemy.Column( sqlalchemy.types.String, nullable=False )
    parent_id_name = sqlalchemy.Column( sqlalchemy.types.String )
    ros_sql_schema_version = sqlalchemy.Column( sqlalchemy.types.Integer )

    timestamps = sqlalchemy.orm.relationship("RosSqlMetadataTimestamps",
                                             order_by="RosSqlMetadataTimestamps.id",
                                             backref=sqlalchemy.orm.backref(ROS_SQL_COLNAME_PREFIX + '_metadata'))
    backrefs = sqlalchemy.orm.relationship("RosSqlMetadataBackrefs",
                                           backref=sqlalchemy.orm.backref(ROS_SQL_COLNAME_PREFIX + '_metadata'))

    def __init__(self, topic_name, table_name, msg_class, msg_md5,is_top,pk_name,parent_id_name):
        self.topic_name = topic_name
        self.table_name = table_name
        self.msg_class_name = msg_class
        self.msg_md5sum = msg_md5
        self.is_top = is_top
        self.pk_name = pk_name
        self.parent_id_name = parent_id_name
        self.ros_sql_schema_version = SCHEMA_VERSION

    def __repr__(self):
        return "<RosSqlMetadata(%r,%r,%r,%r,%r,%r,%r)>"%(
            self.topic_name,
            self.table_name,
            self.msg_class_name,
            self.msg_md5sum,
            self.is_top,
            self.pk_name,
            self.parent_id_name,
            )

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

def parse_field( metadata, topic_name, _type, source_topic_name, field_name,
                 parent_pk_name, parent_pk_type, parent_table_name ):
    """for a given element within a message, find the schema field type"""
    dt = type_map.get(_type,None)
    tab_track_rows = []

    if dt is not None:
        # simple type
        results = {#'assign':dt,
                   'tab_track_rows':tab_track_rows,
                   'col_args': (),
                   'col_kwargs': {'type_':dt},
                   'backref_info_list':[],
                   }
        return results

    other_instance_name = namify( topic_name )

    if _type.endswith('[]'):
        # array - need to start another sql table
        element_type_name = _type[:-2]
        dt = type_map.get(element_type_name,None)
        backref_info_list = []

        if dt is not None:
            # array of fundamental type
            element_class_name = slot_type_to_class_name(element_type_name)
            msg_class = getattr(std_msgs.msg,element_class_name)
            known_sql_type=dt
        else:
            # array of non-fundamental type
            msg_class = roslib.message.get_message_class(element_type_name)
            known_sql_type=None

        rx = generate_schema_raw(metadata,
                                 topic_name, msg_class, top=False,
                                 known_sql_type=known_sql_type,
                                 many_to_one=(parent_table_name,parent_pk_name,parent_pk_type),
                                 )
        bi = {'parent_field':field_name,
              'child_table':rx['table_name'],
              'child_field':rx['foreign_key_column_name'],
              }
        backref_info_list.append( bi )

        tab_track_rows.extend( rx['tracking_table_rows'] )
        other_key_name = other_instance_name + '.' + rx['pk_name']
        results = {
            'tab_track_rows':tab_track_rows,
            'backref_info_list':backref_info_list,
                   }
    else:
        # _type is another message type
        msg_class = roslib.message.get_message_class(_type)
        rx = generate_schema_raw(metadata,topic_name,msg_class, top=False)
        tab_track_rows.extend( rx['tracking_table_rows'] )
        other_key_name = other_instance_name + '.' + rx['pk_name']

        results = {
            'col_args': ( sqlalchemy.ForeignKey(other_key_name,ondelete='cascade'), ),
            'col_kwargs': {'type_':rx['pk_type'],
                           'nullable':False,
                           },
            'tab_track_rows':tab_track_rows,
            'backref_info_list':[],
                   }
    return results

def add_time_cols(this_table, prefix):
    this_table.append_column(
        sqlalchemy.Column( prefix+'_secs', type_map['uint64'] ))
    this_table.append_column(sqlalchemy.Column(
        prefix+'_nsecs', type_map['uint64'] ))

def generate_schema_raw( metadata,
                         topic_name, msg_class, top=True,
                         many_to_one=None,
                         known_sql_type=None):
    """convert a message type into a Python source code string"""
    tracking_table_rows = []
    timestamp_columns = []
    backref_info_list = []

    table_name = namify( topic_name )
    more_texts = []

    this_table = sqlalchemy.Table( table_name, metadata )

    preferred_pk_name = 'id'
    preferred_parent_id_name = 'parent_id'

    if preferred_pk_name in msg_class.__slots__:
        pk_name = ROS_SQL_COLNAME_PREFIX+table_name+'_id'
    else:
        pk_name = preferred_pk_name

    if preferred_parent_id_name in msg_class.__slots__:
        parent_id_name = ROS_SQL_COLNAME_PREFIX+table_name+'_parent_id'
    else:
        parent_id_name = preferred_parent_id_name

    assert pk_name not in msg_class.__slots__
    assert parent_id_name not in msg_class.__slots__
    assert (ROS_SQL_COLNAME_PREFIX+'_timestamp_secs') not in msg_class.__slots__
    assert (ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs') not in msg_class.__slots__

    pk_type = sqlalchemy.types.Integer

    this_table.append_column(
        sqlalchemy.Column(pk_name,
                          pk_type,
                          primary_key=True))

    foreign_key_column_name = parent_id_name
    if many_to_one is not None:
        parent_table_name, parent_pk_name, parent_pk_type = many_to_one
        # add column referring back to original table
        this_table.append_column(
            sqlalchemy.Column( foreign_key_column_name,
                               sqlalchemy.ForeignKey(parent_table_name+'.'+parent_pk_name,ondelete='cascade'),
                               type_ = parent_pk_type,
                               ))

    if top:
        add_time_cols( this_table, ROS_SQL_COLNAME_PREFIX+'_timestamp' )

    if known_sql_type is not None:
        this_table.append_column(sqlalchemy.Column('data', known_sql_type))
    else:
        for name, _type in zip(msg_class.__slots__, msg_class._slot_types):
            if _type=='time':
                # special type - doesn't map to 2 columns
                add_time_cols( this_table, name )
                timestamp_columns.append( name )
            else:
                results = parse_field( metadata, topic_name+'.'+name, _type, topic_name, name, pk_name, pk_type, table_name )
                tracking_table_rows.extend( results['tab_track_rows'] )

                if 'col_args' in results:
                    this_table.append_column(
                        sqlalchemy.Column(name,
                                          *results['col_args'],
                                          **results['col_kwargs'] ))
                backref_info_list.extend( results['backref_info_list'] )

    # these are the args to RosSqlMetadata:
    tracking_table_rows.append(  {'row_args':(topic_name, table_name, msg_class._type, msg_class._md5sum, top, pk_name, parent_id_name),
                                  'timestamp_colnames':timestamp_columns,
                                  'backref_info_list':backref_info_list} )

    results = {'tracking_table_rows':tracking_table_rows,
               'pk_type':pk_type,
               'pk_name':pk_name,
               'table_name':table_name,
               'parent_id_name':parent_id_name,
               }
    if many_to_one is not None:
        results['foreign_key_column_name']=foreign_key_column_name
    return results

def gen_schema( metadata, topic_name, msg_class):
    # add table(s) to MetaData instance
    rx = generate_schema_raw(metadata,topic_name,msg_class, top=True)
    metadata.create_all( metadata.bind )

    # add table tracking row to MetaData instance
    Base.metadata.create_all( metadata.bind )
    Session = sqlalchemy.orm.sessionmaker(bind=metadata.bind)
    session = Session()

    for new_meta_row_args in rx['tracking_table_rows']:
        args = new_meta_row_args['row_args']
        newts_rows = []
        backref_rows = []
        for ts_row in new_meta_row_args['timestamp_colnames']:
            newts_row = RosSqlMetadataTimestamps(ts_row)
            newts_rows.append(newts_row)
        for bi in new_meta_row_args['backref_info_list']:
            backref_row = RosSqlMetadataBackrefs( bi['parent_field'],
                                                  bi['child_table'],
                                                  bi['child_field'],
                                                  )
            backref_rows.append( backref_row )
        new_meta_row = RosSqlMetadata(*args)
        new_meta_row.timestamps = newts_rows
        new_meta_row.backrefs = backref_rows
        session.add(new_meta_row)
    session.commit()

def add_schemas( metadata, list_of_topics_and_messages ):
    for topic_name, msg_class in list_of_topics_and_messages:
        gen_schema( metadata, topic_name, msg_class )
