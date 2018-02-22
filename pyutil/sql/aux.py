from pyutil.sql.models import Frame, Symbol


def upsert_frame(session, name, frame):
    x = session.query(Frame).filter(Frame.name==name).first()
    if x:
        x.frame=frame
    else:
        session.add(Frame(name=name, frame=frame))

# aux. function to access Symbols by name....
def asset(session, name):
    return session.query(Symbol).filter(Symbol.bloomberg_symbol==name).first()
