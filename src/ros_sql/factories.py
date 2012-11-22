"""given a pre-existing database schema, convert ROS messages back and forth"""
import time
from type_map import type_map
import sqlalchemy

import roslib
import roslib.message
roslib.load_manifest('ros_sql')
import rospy
import ros_sql.models as models
import ros_sql.util as util

ROS_SQL_COLNAME_PREFIX = models.ROS_SQL_COLNAME_PREFIX
ROS_TOP_TIMESTAMP_COLNAME_BASE = models.ROS_TOP_TIMESTAMP_COLNAME_BASE

def get_sql_table( session, metadata, topic_name, prefix=None ):
    """helper function to get table for a given topic name (read and write db)"""
    if prefix is None:
        mymeta = session.query(models.RosSqlMetadata).\
                 filter_by(topic_name=topic_name).one()
    else:
        mymeta = session.query(models.RosSqlMetadata).\
                 filter_by(topic_name=topic_name,prefix=prefix).one()
    return metadata.tables[mymeta.table_name]

def update_parents( session, metadata, update_with_parent, topic_name, pk0, conn ):
    """helper function for tables containing items in a another table's list (write db)"""
    for field_name in update_with_parent:
        child_topic = topic_name + '.' + field_name
        child_info = get_table_info( session, topic_name=child_topic )
        child_table = metadata.tables[child_info['table_name']]
        child_pk_col = getattr( child_table.c, child_info['pk_name'] )

        parent_id_name = child_info['parent_id_name']
        kwargs = {parent_id_name:sqlalchemy.sql.bindparam('backref_id')}

        update_results = child_table.update().\
            where( child_pk_col==sqlalchemy.sql.bindparam('child_id')).\
            values( **kwargs )

        args = []
        for child_id in update_with_parent[field_name]:
            args.append( {'child_id':child_id, 'backref_id':pk0} )

        conn.execute( update_results, *args )


def msg2sql(session, metadata, topic_name, msg, timestamp=None, prefix=None):
    """top-level call to execute commands for saving a message (write db)"""
    this_table = get_sql_table( session, metadata, topic_name, prefix=prefix )

    if timestamp is None:
        timestamp = rospy.Time.from_sec( time.time() )

    engine = metadata.bind
    conn = engine.connect()
    trans = conn.begin()

    try:
        kwargs, atts, update_with_parent = msg2dict(
            session, metadata, topic_name, msg, conn, trans)

        kwargs[ROS_TOP_TIMESTAMP_COLNAME_BASE+'_secs']  = timestamp.secs
        kwargs[ROS_TOP_TIMESTAMP_COLNAME_BASE+'_nsecs'] = timestamp.nsecs
        for name, value in atts:
            kwargs[name] = value

        ins = this_table.insert()

        result = conn.execute(ins, [kwargs])

        pk = result.inserted_primary_key
        assert len(pk)==1
        pk0 = pk[0]
        update_parents( session, metadata, update_with_parent, topic_name, pk0, conn )

        trans.commit()
    except:
        trans.rollback()
        raise

    conn.close()

_table_info_topic_cache = {}
_table_info_table_cache = {}
_timestamp_info_cache = {}
_backref_info_cache = {}
def get_table_info(session, topic_name=None, table_name=None):
    """helper function to get table information (read and write db)"""
    if topic_name is not None:
        try:
            mymeta = _table_info_topic_cache[topic_name]
        except KeyError:
            mymeta = session.query(
                models.RosSqlMetadata).filter_by(topic_name=topic_name).one()
            _table_info_topic_cache[topic_name] = mymeta
            _table_info_table_cache[mymeta.table_name] = mymeta
        if table_name is not None:
            assert mymeta.table_name == table_name
    else:
        assert table_name is not None
        try:
            mymeta = _table_info_table_cache[table_name]
        except KeyError:
            mymeta = session.query(
                models.RosSqlMetadata).filter_by(table_name=table_name).one()
            _table_info_table_cache[table_name] = mymeta
            _table_info_topic_cache[mymeta.topic_name] = mymeta

    assert mymeta.ros_sql_schema_version == models.SCHEMA_VERSION

    try:
        myts = _timestamp_info_cache[mymeta.table_name]
    except KeyError:
        myts = session.query(
            models.RosSqlMetadataTimestamps).filter_by( main_id=mymeta.id ).all()
        _timestamp_info_cache[mymeta.table_name] = myts

    MsgClass = util.get_msg_class(mymeta.msg_class_name)
    timestamp_columns = []
    for tsrow in myts:
        timestamp_columns.append((tsrow.column_base_name, tsrow.is_duration))
    try:
        mybackrefs = _backref_info_cache[mymeta.table_name]
    except KeyError:
        mybackrefs = session.query(models.RosSqlMetadataBackrefs).filter_by( main_id=mymeta.id ).all()
        _backref_info_cache[mymeta.table_name] = mybackrefs
    backref_info_list = []
    for backref in mybackrefs:
        backref_info_list.append( {'parent_field':backref.parent_field,
                                   'child_table':backref.child_table,
                                   'child_field':backref.child_field,
                                   })
    return {'class':MsgClass,
            'top':mymeta.is_top,
            'pk_name':mymeta.pk_name,
            'table_name':mymeta.table_name,
            'topic_name':mymeta.topic_name,
            'prefix':mymeta.prefix,
            'timestamp_columns':timestamp_columns,
            'backref_info_list':backref_info_list,
            'parent_id_name':mymeta.parent_id_name,
            }

def sql2msg(topic_name, result, session, metadata):
    """convert query result into message (read db)"""
    info = get_table_info( session, topic_name=topic_name)
    MsgClass = info['class']
    table_name = info['table_name']
    prefix = info['prefix']
    this_table = metadata.tables[table_name]

    inverses = []
    forwards = {}
    for c in this_table.columns:
        if len(c.foreign_keys):
            forwards[c.name] = c.foreign_keys
    d = dict(result)

    results = {}
    if info['top']:
        # It is a top-level table.
        top_secs = d.pop(ROS_TOP_TIMESTAMP_COLNAME_BASE+'_secs')
        top_nsecs = d.pop(ROS_TOP_TIMESTAMP_COLNAME_BASE+'_nsecs')
        results['timestamp'] = util.time_cols_to_ros( top_secs, top_nsecs )

    d.pop( info['pk_name'] )

    parent_id_name = info['parent_id_name']
    if parent_id_name in d:
        # don't populate resulting message with this
        d.pop( parent_id_name )

    for key in d.keys():
        for fk in forwards.get(key, []):
            field_name = key
            field = getattr(result, field_name)
            new_topic = topic_name + '.' + field_name

            new_info = get_table_info( session, topic_name=new_topic)

            pk_name = new_info['pk_name']

            fk = d.pop(field_name)
            # --------------------

            # get back from SQL
            new_table = get_sql_table( session, metadata, new_topic,
                                       prefix=prefix )

            new_primary_key_column = getattr(new_table.c, pk_name)
            s = sqlalchemy.sql.select([new_table], new_primary_key_column==fk )

            conn = metadata.bind.connect()
            sa_result = conn.execute(s)
            msg_actual_sql = sa_result.fetchone()
            sa_result.close()
            if 1:
                new_msg = sql2msg(new_topic, msg_actual_sql, session, metadata )['msg']
                d[field_name] = new_msg
            conn.close()

    if len(inverses):
        for inv in inverses:
            arr = []
            name = inv.name
            tn2 = topic_name + '.' + name

            tmp1 = getattr( result, name )
            for tmp2 in tmp1:
                value2 = sql2msg(tn2, tmp2, session, metadata)['msg']
                arr.append( value2 )

            assert name not in d
            d[name] = arr

    for field, is_duration in info['timestamp_columns']:
        my_secs =  d.pop(field+'_secs')
        my_nsecs = d.pop(field+'_nsecs')
        d[field] = util.time_cols_to_ros( my_secs, my_nsecs,
                                          is_duration=is_duration)

    for backref in info['backref_info_list']:
        backref_values = get_backref_values( backref['child_table'],
                                             session, metadata )
        d[ backref['parent_field'] ] = backref_values

    for k in d:
        if isinstance( d[k], unicode ):
            try:
                d[k] = str(d[k])
            except UnicodeEncodeError:
                pass
    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def get_backref_values( table_name, session, metadata ):
    """helper function to fill list (read db)"""
    new_info = get_table_info(session, table_name=table_name)
    new_table = metadata.tables[new_info['table_name']]
    new_topic = new_info['topic_name']
    s = sqlalchemy.sql.select([new_table])

    conn = metadata.bind.connect()
    sa_result = conn.execute(s)
    msg_actual_sqls = sa_result.fetchall()
    result = []
    for msg_actual_sql in msg_actual_sqls:
        new_msg = sql2msg(new_topic, msg_actual_sql, session, metadata )['msg']
        result.append(new_msg)
    sa_result.close()
    if len(result):
        # check if we need to strip down to bare data (removing ROS messages)
        msg_class = new_msg.__class__
        if (len(msg_class.__slots__)==1 and msg_class.__slots__[0]=='data'):
            _type = msg_class._slot_types[0]
            if type_map.has_key(_type):
                # Collapse messages in array to raw Python type.
                result = [ri.data for ri in result]
    conn.close()
    return result

def insert_row( session, metadata, topic_name, name, value, conn, trans ):
    """helper function to insert data (write db)"""
    name2 = topic_name + '.' + name
    info = get_table_info( session, topic_name=name2)

    table_name = info['table_name']
    this_table = metadata.tables[table_name]

    if isinstance(value, roslib.message.Message ):
        kwargs, atts, update_with_parent = msg2dict( session, metadata, name2, value, conn, trans )
        kw2 = {}
        for k, row in atts:
            assert k not in kwargs # not already there
            assert k not in kw2 # not overwriting
            kw2[k] = row
        kwargs.update(kw2)
    else:
        kwargs = {'data':value}
        update_with_parent = None

    ins = this_table.insert()

    result = conn.execute(ins, [kwargs])

    pk = result.inserted_primary_key
    assert len(pk)==1
    pk0 = pk[0]

    if update_with_parent is not None:
        update_parents( session, metadata, update_with_parent, name2, pk0, conn)

    return pk0

def msg2dict(session, metadata, topic_name, msg, conn, trans):
    """helper function to convert ROS message to dict (write db)"""
    result = {}
    atts = []
    update_with_parent = {}
    for name, _type in zip(msg.__slots__, msg._slot_types):
        value = getattr(msg, name)
        if _type in type_map:
            # simple type
            if isinstance(value, unicode):
                try:
                    # try to convert to plain string if possible
                    value = str(value)
                except UnicodeEncodeError:
                    pass
            result[name] = value
        elif _type == 'time':
            # special case for time type
            result[name+'_secs'] = value.secs
            result[name+'_nsecs'] = value.nsecs
        elif _type == 'duration':
            # special case for duration type
            result[name+'_secs'] = value.secs
            result[name+'_nsecs'] = value.nsecs
        elif _type.endswith('[]'):
            # special case for array type
            refs = []
            for element in value:
                row = insert_row( session, metadata, topic_name, name, element, conn, trans )
                refs.append(row)
            update_with_parent[name] = refs
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( session, metadata, topic_name, name, value, conn, trans )
            atts.append( (name, row) )
    return result, atts, update_with_parent
