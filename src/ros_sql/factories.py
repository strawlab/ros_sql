"""given a pre-existing database schema, convert ROS messages back and forth"""
import time
from type_map import type_map
import sqlalchemy

import roslib
import roslib.message
roslib.load_manifest('ros_sql')
import rospy
import ros_sql.ros2sql as ros2sql

ROS_SQL_COLNAME_PREFIX = ros2sql.ROS_SQL_COLNAME_PREFIX

def get_sql_table( metadata, topic_name ):
    table_name = ros2sql.namify( topic_name, mode='table')
    return metadata.tables[table_name]

def msg2sql(metadata, topic_name, msg, timestamp=None):
    '''generate commands for saving topic'''
    if timestamp is None:
        timestamp=rospy.Time.from_sec( time.time() )
    kwargs, atts = msg2dict(metadata,topic_name,msg)
    kwargs[ROS_SQL_COLNAME_PREFIX+'_timestamp_secs']=timestamp.secs
    kwargs[ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs']=timestamp.nsecs
    for name,value in atts:
        kwargs[name]=value

    this_table = get_sql_table( metadata, topic_name )
    ins = this_table.insert()

    engine = metadata.bind
    conn = engine.connect()
    trans = conn.begin()
    try:
        result = conn.execute(ins,[kwargs])
        trans.commit()
    except:
        trans.rollback()
        raise

def get_table_info(metadata,topic_name=None,table_name=None):
    Session = sqlalchemy.orm.sessionmaker(bind=metadata.bind)
    session = Session()
    if topic_name is not None:
        mymeta=session.query(ros2sql.RosSqlMetadata).filter_by(topic_name=topic_name).one()
        if table_name is not None:
            assert mymeta.table_name == table_name
    else:
        assert table_name is not None
        mymeta=session.query(ros2sql.RosSqlMetadata).filter_by(table_name=table_name).one()
    myts=session.query(ros2sql.RosSqlMetadataTimestamps).filter_by( main_id=mymeta.id ).all()
    # mymeta=session.query(ros2sql.RosSqlMetadata,ros2sql.RosSqlMetadataTimestamps).\
    #         filter(ros2sql.RosSqlMetadata.topic_name==topic_name).one()
    #         #filter(ros2sql.RosSqlMetadata.id==ros2sql.RosSqlMetadataTimestamps.main_id).\
    MsgClass = ros2sql.get_msg_class(mymeta.msg_class_name)
    timestamp_columns = []
    print '.-='*100
    print 'mymeta: %r'%mymeta
    print '.-='*100
    for tsrow in myts:
        timestamp_columns.append(tsrow.column_base_name)
    mybackrefs=session.query(ros2sql.RosSqlMetadataBackrefs).filter_by( main_id=mymeta.id ).all()
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
            'timestamp_columns':timestamp_columns,
            'backref_info_list':backref_info_list,
            }

def sql2msg(topic_name,result,metadata):
    '''convert query result into message'''
    info = get_table_info( metadata, topic_name=topic_name)
    MsgClass = info['class']
    table_name = info['table_name']
    this_table = metadata.tables[table_name]

    print
    print '::::::::::::::::::::::::::: this_table: %r'%this_table
    #print
    #print dir(this_table)
    #print
    print 'this_table.primary_key',this_table.primary_key
    for c in this_table.columns:
        print c
        print c.name
        print c.foreign_keys
        print c.type
        print
    print
    inverses = []
    forwards = {}
    for c in this_table.columns:
        if len(c.foreign_keys):
            forwards[c.name] = c.foreign_keys
        # for fk in c.foreign_keys:
        #     forwards[fk.name] = fk

    # for relationship in result._descriptor.relationships:
    #     if isinstance( relationship, elixir.relationships.OneToMany ):
    #         inverses.append( relationship )
    #     elif isinstance( relationship, elixir.relationships.ManyToOne ):
    #         cnames = relationship.foreign_key
    #         assert len(cnames)==1
    #         cname = cnames[0]
    #         forwards[cname.name] = relationship
    #     elif isinstance( relationship, elixir.relationships.OneToOne ):
    #         pass
    #     else:
    #         raise NotImplementedError
    print '='*100
    print 'result: %r'%type(result)
    print 'result: %r'%result
    print
    #d=result.to_dict()
    d = dict(result)

    results = {}
    if info['top']:
        # It is a top-level table.
        top_secs = d.pop(ROS_SQL_COLNAME_PREFIX+'_timestamp_secs')
        top_nsecs = d.pop(ROS_SQL_COLNAME_PREFIX+'_timestamp_nsecs')
        results['timestamp'] = rospy.Time( top_secs, top_nsecs )

    my_pk = d.pop( info['pk_name'] )

    for key in d.keys():
        for fk in forwards.get(key,[]):
            field_name = key
            #field_name = fk.target_fullname
            #field = getattr(result,field_name)
            field = getattr(result,field_name)
            new_topic = topic_name + '.' + field_name
            new_table_name = ros2sql.namify( new_topic, mode='table')

            new_info = get_table_info( metadata, topic_name=new_topic)

            pk_name = new_info['pk_name']

            fk = d.pop(field_name)
            # --------------------

            # get back from SQL
            new_table = get_sql_table( metadata, new_topic )

            new_primary_key_column = getattr(new_table.c,pk_name)
            s = sqlalchemy.sql.select([new_table], new_primary_key_column==fk )

            conn = metadata.bind.connect()
            sa_result = conn.execute(s)
            msg_actual_sql = sa_result.fetchone()
            print 'msg_actual_sql',msg_actual_sql
            print '*'*1000
            sa_result.close()
            if 1:
                new_msg = sql2msg(new_topic, msg_actual_sql, metadata )['msg']
                d[field_name] = new_msg


            # if schema.have_topic(new_topic):
            #     # This is a semi-hack to restrict us from going back
            #     # into relationships we already went forward on.
            #     new_msg = sql2msg(new_topic, field, metadata )['msg']
            #     d[field_name] = new_msg

    if len(inverses):
        for inv in inverses:
            arr = []
            name = inv.name
            tn2 = topic_name + '.' + name

            tmp1 = getattr( result, name )
            for tmp2 in tmp1:
                value2 = sql2msg(tn2,tmp2,metadata)['msg']
                arr.append( value2 )

            assert name not in d
            d[name] = arr
    print 'd: %r'%d

    for field in info['timestamp_columns']:
        my_secs =  d.pop(field+'_secs')
        my_nsecs = d.pop(field+'_nsecs')
        my_val = rospy.Time( my_secs, my_nsecs )
        d[field] = my_val

    for backref in info['backref_info_list']:
        backref_values = get_backref_values( backref['child_table'],
                                             backref['child_field'],
                                             my_pk, this_table, metadata )
        d[ backref['parent_field'] ] = backref_values

    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def get_backref_values( table_name, field, parent_pk, parent_table, metadata ):

    new_info = get_table_info(metadata, table_name=table_name)
    new_table = metadata.tables[new_info['table_name']]
    new_topic = new_info['topic_name']
    foreign_key_column = getattr(new_table.c,field)
    s = sqlalchemy.sql.select([new_table], foreign_key_column==parent_pk )

    conn = metadata.bind.connect()
    sa_result = conn.execute(s)
    msg_actual_sqls = sa_result.fetchall()
    result = []
    for msq_actual_sql in msg_actual_sqls:
        print 'msg_actual_sql',msg_actual_sql
        print '*'*1000
        new_msg = sql2msg(new_topic, msg_actual_sql, metadata )['msg']
        result.append(new_msg)
    sa_result.close()
    return result

def insert_row( metadata, topic_name, name, value ):
    name2 = topic_name + '.' + name
    info = get_table_info( metadata, topic_name=name2)

    table_name = info['table_name']
    this_table = metadata.tables[table_name]

    if isinstance(value, roslib.message.Message ):
        kwargs, atts = msg2dict( metadata, name2, value )
        kw2 = {}
        for k,row in atts:
            assert k not in kwargs # not already there
            assert k not in kw2 # not overwriting
            kw2[k]=row
        kwargs.update(kw2)
    else:
        kwargs = {'data':value}

    ins = this_table.insert()

    engine = metadata.bind
    conn = engine.connect()
    trans = conn.begin()
    try:
        result = conn.execute(ins,[kwargs])
        trans.commit()
    except:
        trans.rollback()
        raise

    pk = result.inserted_primary_key
    assert len(pk)==1
    pk0 = pk[0]
    return pk0

def msg2dict(metadata,topic_name,msg):
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
                row = insert_row( metadata, topic_name, name, element )
                refs.append(row)
            result[name] = refs
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( metadata, topic_name, name, value )
            atts.append( (name, row) )
    return result, atts
