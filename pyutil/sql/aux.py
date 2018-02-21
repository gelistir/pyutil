from pyutil.sql.models import Frame


def upsert_frame(session, name, frame):
    x = session.query(Frame).filter(Frame.name==name).first()
    if x:
        x.frame=frame
    else:
        session.add(Frame(name=name, frame=frame))