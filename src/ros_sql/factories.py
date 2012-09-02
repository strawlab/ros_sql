import elixir
import time
import importlib
import schema

from type_map import type_map

import roslib; roslib.load_manifest('rospy'); import rospy

def msg2sql(topic_name, msg,timestamp=None,session=None):
    '''generate commands for saving topic /test_pose'''
    if timestamp is None:
        timestamp=rospy.Time.from_sec( time.time() )
    kwargs, atts = msg2dict(topic_name,msg)
    kwargs['record_stamp_secs']=timestamp.secs
    kwargs['record_stamp_nsecs']=timestamp.nsecs
    for name,value in atts:
        kwargs[name]=value
    klass = schema.get_class(topic_name)
    row = klass(**kwargs)
    if session is not None:
        session.add(row)
        session.commit()

def sql2msg(topic_name,result):
    '''convert query result into message'''
    print "1st query result: %r"%result
    print '1st result._descriptor: %r'%result._descriptor
    print '1st dir(result._descriptor)',dir(result._descriptor)
    print '1st result._descriptor.relationships: %r'%result._descriptor.relationships
    inverses = []
    forwards = {}
    for relationship in result._descriptor.relationships:
        print '  relationship',relationship
        print '  isinstance( relationship, elixir.relationships.ManyToOne )',isinstance( relationship, elixir.relationships.ManyToOne )
        print '  isinstance( relationship, elixir.relationships.OneToMany )',isinstance( relationship, elixir.relationships.OneToMany )
        print '  isinstance( relationship, elixir.relationships.OneToOne )',isinstance( relationship, elixir.relationships.OneToOne )
        print

        if isinstance( relationship, elixir.relationships.OneToMany ):
            inverses.append( relationship )
        elif isinstance( relationship, elixir.relationships.ManyToOne ):
            if 1:
                print
                print '*'*100
                print 'relationship',relationship
                #print 'dir(relationship)',dir(relationship)
                #for k in dir(relationship):
                #    print '            %r: %r'%(k, getattr(relationship,k))
                #print '.'*100
            if 1:
                cnames = relationship.foreign_key
                #print 'cnames',cnames
                assert len(cnames)==1
                cname = cnames[0]
                print 'cname.name',cname.name
                forwards[cname.name] = relationship
                #print 'dir(cname)',dir(cname)
                print '*'*100

            if 0:
                cnames = relationship.target_column
                print 'cnames',cnames
                assert len(cnames)==1
                cname = cnames[0]
                assert cname not in forwards
                forwards[cname] = relationship
                print 'adding column %r'%cname
        elif isinstance( relationship, elixir.relationships.OneToOne ):
            pass
        else:
            raise NotImplementedError
    d=result.to_dict()
    print d
    top_level=isinstance(result, schema.RecordedEntity)

    results = {}
    if top_level:
        results['timestamp']=rospy.Time( d.pop('record_stamp_secs'), d.pop('record_stamp_nsecs') )
        # this is a hack to use elixir defaults. XXX TODO: use elixir metadata
        d.pop('recordedentity_id')
        d.pop('row_type')
    d.pop('id') # hack to use elixir default. XXX TODO: get primary key programatically

    for key in d.keys():
        if key in forwards:

        # # XXX TODO dereference properly by using results.descriptor.relationships?
        # if key.endswith('_id'):  # this is a hack. should dereference properly.
            print 'key',key
            relationship = forwards[key]
            # field_name = key[:-3] # this is a hack. should dereference properly.
            # print 'field_name',field_name
            # if not hasattr(result,field_name): # a hack on a hack
            #     # no, this isn't a reference to another table
            #     continue
            field_name = relationship.name
            field = getattr(result,field_name)
            #print '                     relationship.name',relationship.name
            #field = getattr(result, relationship.name)
            print '                     field',field
            new_topic = topic_name + '.' + field_name
            print 'new_topic',new_topic
            d.pop(key)
            if schema.have_topic(new_topic):
                # This is a semi-hack to restrict us from going back
                # into relationships we already went forward on.
                print '  ...==========>>>>>> recursive call into sql2msg()'
                new_msg = sql2msg(new_topic, field )['msg']
                print 'new_msg',new_msg
                d[field_name] = new_msg
            # print 'fk',fk

    for key in d.keys():
        # XXX TODO use out-of-band channel to store time fields rather than name munging.
        if key.endswith('_secs'):
            # hack to detect encoded time: 2 fields with same name execpt ending
            name = key[:-5]

            name_sec = name + '_secs'
            name_nsec = name + '_nsecs'
            if not name_nsec in d:
                continue
            time_sec = d.pop(name_sec)
            time_nsec = d.pop(name_nsec)
            value = rospy.Time(time_sec,time_nsec)
            assert name not in d
            d[name] = value

    try:
        msg_class_name = schema.get_msg_name(topic_name)
    except KeyError, e:
        print 'bad key: %r'%topic_name
        9238/0
    MsgClass = get_msg_class(msg_class_name)

    if len(inverses):
        # XXX TODO: how to add ManyToOne things that target this row? This
        # is specifically for arrays.
        for inv in inverses:
            arr = []
            name = inv.name
            tn2 = topic_name + '.' + name

            print '----------------inverse name: %s'%name
            print '----------------tn2: %r'%tn2
            print '----------------relationship to sort out: %r'%inv

            tmp1 = getattr( result, name )
            print ' tmp1: %r'%tmp1
            for tmp2 in tmp1:
                print '                    tmp2: %r'%tmp2
                value2 = sql2msg(tn2,tmp2)['msg']
                arr.append( value2 )

            assert name not in d
            d[name] = arr

    if 1:
        print 'query result: %r'%result
        print 'result._descriptor: %r'%result._descriptor
        print 'result._descriptor.relationships: %r'%result._descriptor.relationships
        print 'MsgClass',MsgClass
        print 'dir(MsgClass)',dir(MsgClass)
        print 'd',d

    msg = MsgClass(**d)
    results['msg'] = msg
    return results

def insert_row( topic_name, name, value ):
    name2 = topic_name + '.' + name
    klass = schema.get_class(name2)

    # print 'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'
    # print 'name2',name2
    # print 'value',value
    kwargs, atts = msg2dict( name2, value )
    # print 'kwargs'
    # print kwargs
    # print 'name2',name2
    # print 'atts'
    # print atts
    # print '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'
    kw2 = {}
    for k,v in atts:
        assert k not in kwargs # not already there
        assert k not in kw2 # not overwriting
        kw2[k]=v
    kwargs.update(kw2)
    row = klass(**kwargs)
    # recursivley do atts
    #if len(atts):
    #    raise NotImplementedError
    return row

def msg2dict(topic_name,msg):
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
                row = insert_row( topic_name, name, element )
                refs.append(row)
            result[name] = refs
        else:
            # compound type
            #result[name] = msg2dict( value )
            row = insert_row( topic_name, name, value )
            atts.append( (name, row) )
    return result, atts

def get_msg_class(msg_name):
    p1,p2 = msg_name.split('/')
    module_name = p1+'.msg'
    class_name = p2
    module = importlib.import_module(module_name)
    klass = getattr(module,class_name)
    return klass
