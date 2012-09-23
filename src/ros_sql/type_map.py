import sqlalchemy.types as s
import ros_sql.models as models

type_map = {
    'bool':
        s.Boolean(),
    'char':
        s.SmallInteger(unsigned=True),
    'int8':
        s.SmallInteger(),
    'uint8':
        s.SmallInteger(unsigned=True),
    'byte':
        s.SmallInteger(),
    'int16':
        s.Integer(),
    'uint16':
        s.Integer(unsigned=True),
    'int32':
        s.Integer(),
    'uint32':
        s.Integer(unsigned=True),
    'int64':
        s.BigInteger(),
    'uint64':
        s.BigInteger(unsigned=True),
    'float32':
        s.Float(precision=32),
    'float64':
        s.Float(precision=64),
    'string':
        s.Text(),
    }
