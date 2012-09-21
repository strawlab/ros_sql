import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.types

ROS_SQL_COLNAME_PREFIX = '_ros_sql'
ROS_TOP_TIMESTAMP_COLNAME_BASE = ROS_SQL_COLNAME_PREFIX+'_timestamp'
SCHEMA_VERSION = 2

Base = sqlalchemy.ext.declarative.declarative_base()

class RosSqlMetadata(Base):
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_metadata'
    id = sqlalchemy.Column( sqlalchemy.types.Integer, primary_key=True)
    topic_name = sqlalchemy.Column( sqlalchemy.types.String)
    table_name = sqlalchemy.Column( sqlalchemy.types.String)
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

    def is_equal(self,other):
        # This could probably be renamed __eq__, but I don't know if sqlalchemy needs __eq__
        def check_attr(self, other, attr):
            self_list = getattr(self,attr)
            other_list = getattr(other,attr)
            if len(self_list) != len(other_list):
                return False
            # XXX This enforces order on the equality check. Do we need that?
            for i,o in zip(self_list,other_list):
                if not i.is_equal(o):
                    return False
            return True

        still_equal = True
        still_equal &= check_attr( self, other, 'timestamps')
        still_equal &= check_attr( self, other, 'backrefs')
        return (still_equal and
                self.topic_name             == other.topic_name             and
                self.table_name             == other.table_name             and
                self.msg_class_name         == other.msg_class_name         and
                self.msg_md5sum             == other.msg_md5sum             and
                self.is_top                 == other.is_top                 and
                self.pk_name                == other.pk_name                and
                self.parent_id_name         == other.parent_id_name         and
                self.ros_sql_schema_version == other.ros_sql_schema_version )

class RosSqlMetadataBackrefs(Base):
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_backref_metadata'
    id =  sqlalchemy.Column(sqlalchemy.types.Integer, primary_key=True)
    parent_field = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)
    child_table = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)
    child_field = sqlalchemy.Column( sqlalchemy.types.String, nullable=False)

    main_id = sqlalchemy.Column( sqlalchemy.types.Integer,
                                 sqlalchemy.ForeignKey(ROS_SQL_COLNAME_PREFIX + '_metadata.id' ))
                                 #nullable=False)
    def __init__(self, parent_field, child_table, child_field):
        self.parent_field = parent_field
        self.child_table = child_table
        self.child_field = child_field
    def __repr__(self):
        return '<RosSqlMetadataBackrefs(%r,%r,%r)'%(
            self.parent_field, self.child_table, self.child_field)
    def is_equal(self,other):
        return (self.parent_field == other.parent_field and
                self.child_table  == other.child_table  and
                self.child_field  == other.child_field)

class RosSqlMetadataTimestamps(Base):
    """keep track of names of Time and Duration fields"""
    __tablename__ = ROS_SQL_COLNAME_PREFIX + '_timestamp_metadata'
    id =  sqlalchemy.Column(sqlalchemy.types.Integer, primary_key=True)
    column_base_name = sqlalchemy.Column( sqlalchemy.types.String,
                                          nullable=False )
    is_duration = sqlalchemy.Column( sqlalchemy.types.Boolean, nullable=False )

    main_id = sqlalchemy.Column( sqlalchemy.types.Integer,
                                 sqlalchemy.ForeignKey(ROS_SQL_COLNAME_PREFIX + '_metadata.id' ))
                                 #nullable=False)

    def __init__(self, column_base_name, is_duration):
        self.column_base_name = column_base_name
        self.is_duration = is_duration
    def __repr__(self):
        return '<RosSqlMetadataTimestamps(%r,%r)>'%(self.column_base_name,self.is_duration)
    def is_equal(self,other):
        return (self.column_base_name == other.column_base_name and
                self.is_duration      == other.is_duration)
