import sqlalchemy

def get_session(metadata):
    # This is likely not 100% sqlalchemy best practices compliant, but
    # at least it keeps the session instantiation all in one place.
    Session = sqlalchemy.orm.sessionmaker(bind=metadata.bind)
    session = Session()
    return session
