import time

import pymongo
from sqlalchemy import Column, String, create_engine, Integer, DateTime, and_, TIMESTAMP, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.declarative import declarative_base

# 创建对象的基类:
from utils.config import zydb, MONGODB_URI, MONGODB_DBNAME, MONGODB_DOCNAME, pydb

Base = declarative_base()


class QytCompanyInfo(Base):
    __tablename__ = 'qyt_company_info'
    company_id = Column(Integer, primary_key=True)
    company_name = Column(String(255))
    credit_code = Column(String(100))
    org_code = Column(String(100))
    operate_address = Column(String(255))
    company_type = Column(String(100))
    website = Column(String(100))
    legalperson_name = Column(String(150))
    register_capital = Column(String(50))
    telephone = Column(String(50))
    mailbox = Column(String(255))
    operate_state = Column(Integer)
    operate_term = Column(String(100))
    industry = Column(String(100))
    once_name = Column(String(255))
    operate_range = Column(String(2000))
    approval_time = Column(String(100))
    register_office = Column(String(255))
    establish_time = Column(DateTime)
    area_code = Column(String(100))
    area_name = Column(String(100))
    reg_addr = Column(String(128))
    synopsis = Column(String(4000))
    logo_url = Column(String(255))
    create_time = Column(Integer)
    update_time = Column(Integer)
    md5 = Column(String(100))
    yn_del = Column(Integer)
    browse_count = Column(Integer)

    def get_company_name(self):
        return self.company_name


class QytCompanyAptitude(Base):
    __tablename__ = 'qyt_company_aptitude'
    aptitude_id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    aptitude_type = Column(String(50))
    aptitude_number = Column(String(100))
    aptitude_name = Column(String(100))
    award_date = Column(DateTime)
    invalid_date = Column(DateTime)
    award_office = Column(String(255))
    create_time = Column(TIMESTAMP)
    update_time = Column(TIMESTAMP)
    md5 = Column(String(100))
    yn_del = Column(Integer)
    remarks = Column(String(50))


class NewCompQiYe(Base):
    __tablename__ = 'new_comp_qiye'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    code = Column(String(255))
    uuid = Column(Integer)
    pid = Column(String(255))


class QytCompanyAptitudeChange(Base):
    __tablename__ = 'qyt_company_aptitude_change'
    company_id = Column(Integer, primary_key=True)
    aptitude_number = Column(String(100))
    change_date = Column(DateTime)
    operation = Column(String(100))
    change_first = Column(String(255))
    change_first_org = Column(String(255))
    change_after = Column(String(255))
    change_after_org = Column(String(255))
    create_time = Column(TIMESTAMP)


class MysqlSession(object):
    def __init__(self, uri):
        self.engine = create_engine(uri, echo=False, max_overflow=5)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_factory)

    def __call__(self, *args, **kwargs):
        return self.session()


class MongoDB(object):
    def __init__(self):
        self.client = pymongo.MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DBNAME]
        self.collection = self.db[MONGODB_DOCNAME]


if __name__ == "__main__":
    db = MysqlSession(pydb)()
    # resp = db.query(QytCompanyInfo.company_id).filter_by(company_name='中莲建设有限公司').first()
    # print(resp)
    # # for i in db.query(QytCompanyInfo).filter(
    # #         and_(QytCompanyInfo.area_code.like("320000%"), QytCompanyInfo.company_name.notlike("%公司"))):
    # #     print(i.get_company_name())
    try:
        data = db.query(NewCompQiYe).filter_by(code=None,uuid=None,pid=None).all()
        print(len(data))
        for index,d in enumerate(data,start=1):
            print(index)
            db.delete(d)
            db.commit()
    except Exception as e:
        db.rollback()
        print(e)
