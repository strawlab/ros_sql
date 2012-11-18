from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

def get_session(metadata):
    # This is likely not 100% sqlalchemy best practices compliant, but
    # at least it keeps the session instantiation all in one place.
    session_factory = sessionmaker(bind=metadata.bind)
    Session = scoped_session(session_factory)
    session = Session()
    return session
