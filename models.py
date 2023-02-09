import pymysql.connections
from google.cloud.sql.connector import Connector
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Model = declarative_base(name='Model')
session = None


class Semmed(Model):
    __tablename__ = 'semmed'
    id = Column(Integer, primary_key=True)
    pmid = Column(String(45))
    sen_start_index = Column(Integer)
    sentence = Column(String(2000))
    sen_end_index = Column(Integer)
    predicate = Column(String(45))
    subject_cui = Column(String(45))
    subject_name = Column(String(250))
    object_cui = Column(String(45))
    object_name = Column(String(250))
    subject_start_index = Column(Integer)
    subject_end_index = Column(Integer)
    predicate_start_index = Column(Integer)
    predicate_end_index = Column(Integer)

    def __init__(self, pmid, sen_start_index, sentence, sen_end_index, predicate,
                 subject_cui, subject_name, object_cui, object_name,
                 subject_start_index, subject_end_index, predicate_start_index, predicate_end_index):
        self.pmid = pmid
        self.sen_start_index = sen_start_index
        self.sentence = sentence
        self.sen_end_index = sen_end_index
        self.predicate = predicate
        self.subject_cui = subject_cui
        self.subject_name = subject_name
        self.object_cui = object_cui
        self.object_name = object_name
        self.subject_start_index = subject_start_index
        self.subject_end_index = subject_end_index
        self.predicate_start_index = predicate_start_index
        self.predicate_end_index = predicate_end_index


def init_db(instance: str, database: str) -> None:  # pragma: no cover
    connector = Connector()

    def get_conn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = connector.connect(
            instance_connection_string=instance,
            driver='pymysql',
            user='exporter',
            password='34mtQnL6tjxaLDZd',
            database=database
        )
        return conn

    engine = create_engine('mysql+pymysql://', creator=get_conn, echo=False)
    global session
    session = sessionmaker()
    session.configure(bind=engine)
