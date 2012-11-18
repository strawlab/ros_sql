#!/usr/bin/env python
import sys
import re

import sqlalchemy
import sqlalchemy.types

from type_map import type_map

import roslib
roslib.load_manifest('ros_sql')
import rospy
import std_msgs
from ros_sql.models import ROS_SQL_COLNAME_PREFIX, \
     ROS_TOP_TIMESTAMP_COLNAME_BASE, Base, \
     RosSqlMetadata, RosSqlMetadataBackrefs, RosSqlMetadataTimestamps
import ros_sql.util as util

def add_schemas( session, metadata, list_of_topics_and_messages, prefix=None ):
    """create tables for multiple topics and messages"""
    for topic_name, msg_class in list_of_topics_and_messages:
        gen_schema( session, metadata, topic_name, msg_class, prefix=prefix )

def gen_schema( session, metadata, topic_name, msg_class, prefix=None):
    """create a SQL table(s) for a given topic and message class"""
    # add table(s) to MetaData instance
    rx = generate_schema_raw(session, metadata,topic_name,msg_class, top=True,
                             prefix=prefix)
    metadata.create_all( metadata.bind )

    # add table tracking row to MetaData instance
    Base.metadata.create_all( metadata.bind )

    for new_meta_row_args in rx['tracking_table_rows']:
        args = new_meta_row_args['row_args']
        newts_rows = []
        backref_rows = []
        for ts_row,is_duration in new_meta_row_args['timestamp_colnames']:
            newts_row = RosSqlMetadataTimestamps(ts_row,is_duration)
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

        # if equivalent is already in database, don't add
        old_meta_rows=session.query(RosSqlMetadata).filter_by(topic_name=new_meta_row.topic_name).all()
        if len(old_meta_rows):
            assert len(old_meta_rows)==1
            old_meta_row = old_meta_rows[0]
            if not old_meta_row.is_equal(new_meta_row):
                raise MetadataChangedError(old_meta_row, new_meta_row)
        else:
            # this metadata is not already present - add it
            session.add(new_meta_row)

    session.commit()

def generate_schema_raw( session, metadata,
                         topic_name, msg_class, top=True,
                         many_to_one=None,
                         known_sql_type=None,
                         prefix=None):
    """convert a message type into an SQL database schema"""
    tracking_table_rows = []
    timestamp_columns = []
    backref_info_list = []

    table_name = util.namify( topic_name )
    if prefix is not None:
        table_name = prefix + table_name

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
    assert (ROS_TOP_TIMESTAMP_COLNAME_BASE+'_secs') not in msg_class.__slots__
    assert (ROS_TOP_TIMESTAMP_COLNAME_BASE+'_nsecs') not in msg_class.__slots__

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
        util.add_time_cols( this_table, ROS_TOP_TIMESTAMP_COLNAME_BASE )

    if known_sql_type is not None:
        this_table.append_column(sqlalchemy.Column('data', known_sql_type))
    else:
        for name, _type in zip(msg_class.__slots__, msg_class._slot_types):
            if _type=='time':
                # special type - doesn't map to 2 columns
                util.add_time_cols( this_table, name )
                timestamp_columns.append( (name,False) )
            elif _type=='duration':
                # special type - doesn't map to 2 columns
                util.add_time_cols( this_table, name, duration=True )
                timestamp_columns.append( (name,True) )
            else:
                results = parse_field( session, metadata, topic_name+'.'+name,
                                       _type, topic_name, name, pk_name,
                                       pk_type, table_name, prefix=prefix )
                tracking_table_rows.extend( results['tab_track_rows'] )

                if 'col_args' in results:
                    this_table.append_column(
                        sqlalchemy.Column(name,
                                          *results['col_args'],
                                          **results['col_kwargs'] ))
                backref_info_list.extend( results['backref_info_list'] )

    # these are the args to RosSqlMetadata:
    tracking_table_rows.append(  {'row_args':(topic_name, table_name, prefix, msg_class._type, msg_class._md5sum, top, pk_name, parent_id_name),
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

def parse_field( session, metadata, topic_name, _type, source_topic_name,
                 field_name, parent_pk_name, parent_pk_type, parent_table_name,
                 prefix=None ):
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

    other_instance_name = util.namify( topic_name )
    if prefix is not None:
        other_instance_name = prefix + other_instance_name

    if _type.endswith('[]'):
        # array - need to start another sql table
        element_type_name = _type[:-2]
        dt = type_map.get(element_type_name,None)
        backref_info_list = []

        if dt is not None:
            # array of fundamental type
            element_class_name = util.slot_type_to_class_name(element_type_name)
            msg_class = getattr(std_msgs.msg,element_class_name)
            known_sql_type=dt
        else:
            # array of non-fundamental type
            msg_class = roslib.message.get_message_class(element_type_name)
            known_sql_type=None

        rx = generate_schema_raw(session, metadata,
                                 topic_name, msg_class, top=False,
                                 known_sql_type=known_sql_type,
                                 many_to_one=(parent_table_name,parent_pk_name,parent_pk_type),
                                 prefix=prefix,
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
        rx = generate_schema_raw(session, metadata,topic_name,msg_class,
                                 top=False, prefix=prefix)
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

class MetadataChangedError(RuntimeError):
    def __init__(self,old_meta_row, new_meta_row):
        super(MetadataChangedError,self).__init__(
            'metadata changed (original topic %r, new topic %r): %r -> %r'%
            (old_meta_row.topic_name, new_meta_row.topic_name,
             old_meta_row,new_meta_row))
        self.old_meta_row = old_meta_row
        self.new_meta_row = new_meta_row
