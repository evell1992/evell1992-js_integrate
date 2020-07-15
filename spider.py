import copy
import random
import threading
import time
import traceback
from datetime import datetime
from itertools import chain
from pprint import pprint

import requests
from queue import Queue
from sqlalchemy import and_
from utils.logger import logger
from utils.config import pydb, zydb
from utils.context import pool, crawl_handler, clean_handler, snow_handler
from utils.db_action import QytCompanyInfo, MysqlSession, MongoDB, QytCompanyAptitude, QytCompanyAptitudeChange, \
    NewCompQiYe
from utils.snow_flake import IdWorker


class JsIntegrateSpider(object):
    USER_AGENTS = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    ]

    @classmethod
    def headers(cls):
        return {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '14',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': '221.226.118.170:8080',
            'Origin': 'http://221.226.118.170:8080',
            'User-Agent': '{}'.format(cls.USER_AGENTS[random.randint(0, len(cls.USER_AGENTS) - 1)]),
            'X-Requested-With': 'XMLHttpRequest'
        }

    @classmethod
    def download(cls, url, params, retry=5):
        error = ''
        while retry:
            try:
                resp = requests.post(url=url, data=params, headers=cls.headers())
                status_code = resp.status_code
                assert status_code == 200
                result = resp.json()
                return result
            except Exception as e:
                retry -= 1
                error = 'download error:{}'.format(traceback.format_exc())
        logger.error(error)

    @classmethod
    def crawl_certificate(cls, keyword, page=1):
        url = 'http://221.226.118.170:8080/entpcertlist/queryEntpCertList'
        data = {
            'entpname': '{}'.format(keyword),
            'certcode': '',
            'status': '有效',
            'startdate': '',
            'enddate': '',
            'page': '{}'.format(page),
            'rows': '50'
        }
        resp = cls.download(url=url, params=data)
        if resp:
            company_certificate = resp.get('rows')
            total_page = resp.get('pager').get('maxPage')
            yield total_page, company_certificate
        else:
            logger.warning('keyword: {}, page: {} not response'.format(keyword, page))
            yield 0, []

    @classmethod
    def crawl_qualifications(cls, company_certificate, page=1):
        entpcode = company_certificate.get('entpcode')
        certcode = company_certificate.get('certcode')
        url = 'http://221.226.118.170:8080/entpcertlist/queryQualType/{}/{}'.format(entpcode, certcode)
        data = {
            'page': '{}'.format(page),
            'rows': '50'
        }
        resp = cls.download(url=url, params=data)
        if resp:
            certificate_qualifications = resp.get('rows')
            total_page = resp.get('pager').get('maxPage')
            yield total_page, certificate_qualifications
        else:
            logger.warning('{}:{} - certificate:{} not response'.format(
                company_certificate.get('entpname'),
                company_certificate.get('entpcode'),
                company_certificate.get('certcode')))
            yield 0, []

    @classmethod
    def start_crawl(cls, keyword):
        total_page, certificate = next(cls.crawl_certificate(keyword))
        for total, certificate in chain([(total_page, certificate)],
                                        *(cls.crawl_certificate(keyword=keyword, page=i) for i in
                                          range(2, total_page + 1))):
            for _ in certificate:
                qualifications_page, qualifications = next(cls.crawl_qualifications(_))
                full_qualifications = chain(
                    *(qualification for total, qualification in chain(
                        [(qualifications_page, qualifications)], *(
                            cls.crawl_qualifications(_, page=i) for i in range(2, qualifications_page + 1)))))
                qualifications = [{'applytag': i.get('applytag'),
                                   'approvedate': datetime.strptime(i.get('approvedate'), '%Y-%m-%d %H:%M:%S'),
                                   'aptitude_name': '{}{}{}'.format(i.get('majorname'),
                                                                    i.get('tradename'),
                                                                    i.get('levelname', '').replace(
                                                                        '壹', '一').replace(
                                                                        '贰', '二').replace(
                                                                        '叁', '三').replace('肆', '四'))
                                   } for i in full_qualifications]
                if qualifications:
                    tmp = {
                        'certcode': _.get('certcode'),
                        'entpcode': _.get('entpcode'),
                        'entpname': _.get('entpname'),
                        'orgname': _.get('orgname'),
                        'validdate': datetime.strptime(_.get('validdate'), '%Y-%m-%d %H:%M:%S'),
                        'qualifications': qualifications
                    }
                    yield tmp


class DataProducer(object):
    def __init__(self, id_queue):
        self.p_session = MysqlSession(pydb)
        self.z_session = MysqlSession(zydb)
        self.mongo = MongoDB()
        self.zydb = self.z_session()
        self.pydb = self.p_session()
        self.id_queue = id_queue

    @staticmethod
    def get_today():
        return datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')

    def check_old_data(self, result):
        old_data = self.mongo.collection.find_one({"_id": result.get('certcode')}, {'_id': 0, 'crawl_time': 0})
        if result == old_data:
            return False
        else:
            return True

    def query_company_id(self, company_name):
        company_info = self.zydb.query(QytCompanyInfo.company_id).filter_by(
            company_name=company_name).first()
        if company_info:
            return company_info[0]
        else:
            return None

    def query_company_id_by_aptitude_number(self, aptitude_number):
        info = self.zydb.query(QytCompanyAptitude.company_id).filter_by(
            aptitude_number=aptitude_number).first()
        if info:
            return info[0]
        else:
            return None

    def query_aptitude_info(self, aptitude_number):
        info = self.zydb.query(QytCompanyAptitude.aptitude_number, QytCompanyAptitude.aptitude_name,
                               QytCompanyAptitude.award_date, QytCompanyAptitude.invalid_date,
                               QytCompanyAptitude.award_office, QytCompanyAptitude.remarks).filter_by(
            aptitude_number=aptitude_number)
        return {i[1]: {
            'certcode': i[0],
            'aptitude_name': i[1],
            'approvedate': i[2],
            'validdate': i[3],
            'orgname': i[4],
            'applytag': i[5] if i[5] else '',
        } for i in info}

    def query_new_company_info(self, name):
        info = self.pydb.query(NewCompQiYe.name).filter_by(name=name).first()
        if info:
            return True
        else:
            return False

    def check_changed_aptitude(self, result):
        new_aptitude = {i.get('aptitude_name'): {
            'certcode': result.get('certcode'),
            'aptitude_name': i.get('aptitude_name'),
            'approvedate': i.get('approvedate'),
            'validdate': result.get('validdate'),
            'orgname': result.get('orgname'),
            'applytag': i.get('applytag'),
        } for i in result.get('qualifications')}
        old_aptitude = self.query_aptitude_info(result.get('certcode'))
        add_dict = dict()
        delete_dict = dict()
        update_dict = dict()
        for k, v in new_aptitude.items():
            old_v = old_aptitude.get(k)
            if old_v:
                old_aptitude.pop(k)
                if old_v != v:
                    update_dict[k] = (old_v, v)
            else:
                add_dict[k] = v
        [delete_dict.update({k: v}) for k, v in old_aptitude.items()]
        return add_dict, delete_dict, update_dict

    def add_aptitude_info(self, values, company_id):
        for k, v in values.items():
            new_info = QytCompanyAptitude(
                aptitude_id=self.id_queue.get(),
                company_id=company_id,
                aptitude_type='建筑业企业资质',
                aptitude_number=v['certcode'],
                aptitude_name=v['aptitude_name'],
                award_date=v['approvedate'],
                invalid_date=v['validdate'],
                award_office=v['orgname'],
                remarks=v['applytag'],
                create_time=datetime.now()
            )
            self.zydb.add(new_info)
            change_record = QytCompanyAptitudeChange(
                company_id=company_id,
                aptitude_number=v['certcode'],
                change_date=self.get_today(),
                operation='新增',
                change_after_org='{},{},{},{},{}'.format(
                    v.get('aptitude_name', ''),
                    datetime.strftime(v.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(v.get('validdate', datetime.now()), "%Y-%m-%d"),
                    v.get('orgname', ''),
                    v.get('applytag', '')),
                change_after='资质名称:{}<br>发证日期:{}<br>发证有效期:{}<br>取得方式:{}<br>'.format(
                    v.get('aptitude_name', ''),
                    datetime.strftime(v.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(v.get('validdate', datetime.now()), "%Y-%m-%d"),
                    v.get('applytag', '')),
                create_time=datetime.now()
            )
            self.zydb.add(change_record)
            self.zydb.flush()

    def delete_aptitude_info(self, values, company_id):
        for k, v in values.items():
            self.zydb.query(QytCompanyAptitude).filter_by(
                aptitude_number=v['certcode'],
                aptitude_name=v['aptitude_name']).delete()
            change_record = QytCompanyAptitudeChange(
                company_id=company_id,
                aptitude_number=v['certcode'],
                change_date=self.get_today(),
                operation='删除',
                change_first_org='{},{},{},{},{}'.format(
                    v.get('aptitude_name', ''),
                    datetime.strftime(v.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(v.get('validdate', datetime.now()), "%Y-%m-%d"),
                    v.get('orgname', ''),
                    v.get('applytag', '')),
                change_first='资质名称:{}<br>发证日期:{}<br>发证有效期:{}<br>取得方式:{}<br>'.format(
                    v.get('aptitude_name', ''),
                    datetime.strftime(v.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(v.get('validdate', datetime.now()), "%Y-%m-%d"),
                    v.get('applytag', '')),
                create_time=datetime.now())
            self.zydb.add(change_record)

    def update_aptitude_info(self, values, company_id):
        for k, v in values.items():
            old_value = v[0]
            new_value = v[1]
            update_info = dict(
                award_date=new_value['approvedate'],
                invalid_date=new_value['validdate'],
                award_office=new_value['orgname'],
                remarks=new_value['applytag'],
                update_time=datetime.now()
            )
            self.zydb.query(QytCompanyAptitude).filter_by(
                aptitude_number=old_value['certcode'],
                aptitude_name=old_value['aptitude_name']).update(update_info)
            change_record = QytCompanyAptitudeChange(
                company_id=company_id,
                aptitude_number=old_value['certcode'],
                change_date=self.get_today(),
                operation='更新',
                change_first_org='{},{},{},{},{}'.format(
                    old_value.get('aptitude_name', ''),
                    datetime.strftime(old_value.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(old_value.get('validdate', datetime.now()), "%Y-%m-%d"),
                    old_value.get('orgname', ''),
                    old_value.get('applytag', '')),
                change_first='资质名称:{}<br>发证日期:{}<br>发证有效期:{}<br>取得方式:{}<br>'.format(
                    old_value.get('aptitude_name', ''),
                    datetime.strftime(old_value.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(old_value.get('validdate', datetime.now()), "%Y-%m-%d"),
                    old_value.get('applytag', '')),
                change_after_org='{},{},{},{},{}'.format(
                    new_value.get('aptitude_name', ''),
                    datetime.strftime(new_value.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(new_value.get('validdate', datetime.now()), "%Y-%m-%d"),
                    new_value.get('orgname', ''),
                    new_value.get('applytag', '')),
                change_after='资质名称:{}<br>发证日期:{}<br>发证有效期:{}<br>取得方式:{}<br>'.format(
                    new_value.get('aptitude_name', ''),
                    datetime.strftime(new_value.get('approvedate', datetime.now()), "%Y-%m-%d"),
                    datetime.strftime(new_value.get('validdate', datetime.now()), "%Y-%m-%d"),
                    new_value.get('applytag', '')),
                create_time=datetime.now())
            self.zydb.add(change_record)

    def update_mysql_aptitude_table(self, result, company_id):
        add_dict, delete_dict, update_dict = self.check_changed_aptitude(result)
        self.add_aptitude_info(add_dict, company_id)
        self.delete_aptitude_info(delete_dict, company_id)
        self.update_aptitude_info(update_dict, company_id)
        self.zydb.commit()
        logger.info("{}: 更新数量:{} 增加数量:{} 删除数量:{}".format(result.get('entpname'), len(update_dict), len(add_dict),
                                                         len(delete_dict)))

    def process_result(self, result):
        try:
            changed = self.check_old_data(result)
            if changed:
                # mysql库里的括号是英文括号，这里转换过之后去比对
                company_id = self.query_company_id_by_aptitude_number(result.get('certcode'))
                if company_id:
                    self.update_mysql_aptitude_table(result, company_id)
                    mongo_update = copy.deepcopy(result)
                    mongo_update['_id'] = mongo_update['certcode']
                    mongo_update['crawl_time'] = datetime.now()
                    self.mongo.collection.update_one({'_id': result.get('certcode')}, {'$set': mongo_update},
                                                     upsert=True)
                else:
                    company_name = result.get('entpname').replace('（', '(').replace('）', ')')
                    _company_id = self.query_company_id(company_name)
                    if _company_id:
                        self.update_mysql_aptitude_table(result, _company_id)
                        mongo_update = copy.deepcopy(result)
                        mongo_update['_id'] = mongo_update['certcode']
                        mongo_update['crawl_time'] = datetime.now()
                        self.mongo.collection.update_one({'_id': result.get('certcode')}, {'$set': mongo_update},
                                                         upsert=True)
                    else:
                        exist = self.query_new_company_info(company_name)
                        if not exist:
                            new_company = NewCompQiYe(name=company_name)
                            self.pydb.add(new_company)
                            self.pydb.commit()
                            logger.info('新增公司：{} - {}'.format(company_name, result.get('certcode')))
                        else:
                            logger.info('新增公司：{} - {} 已存在'.format(company_name, result.get('certcode')))
            else:
                self.mongo.collection.update_one({'_id': result.get('certcode')},
                                                 {'$set': {'crawl_time': datetime.now()}})
        except Exception as e:
            self.zydb.rollback()
            self.pydb.rollback()
            raise e

    def __del__(self):
        self.zydb.close()
        self.pydb.close()


class Worker(object):

    def __init__(self):
        self.spider = JsIntegrateSpider
        self.result_queue = Queue()
        self.keyword_queue = Queue()
        self._id_queue = Queue(maxsize=50)
        self.zydb = MysqlSession(zydb)

    def generate_keyword(self):
        session = self.zydb()
        self.keyword_queue.put('公司')
        for i in session.query(QytCompanyInfo).filter(
                and_(QytCompanyInfo.area_code.like("320000%"), QytCompanyInfo.company_name.notlike("%公司"))):
            self.keyword_queue.put(i.get_company_name())

    @pool(1, 'snow')
    @snow_handler
    def generate_id(self):
        id_worker = IdWorker(1, 2, 0)
        while True:
            _id = id_worker.get_id()
            self._id_queue.put(_id, block=False)

    @pool(1, 'spider')
    @crawl_handler
    def crawl(self):
        keyword = self.keyword_queue.get(block=False)
        count = 0
        for count, info in enumerate(self.spider.start_crawl(keyword), start=1):
            self.result_queue.put(info)
        return keyword, count

    @pool(1, 'clean')
    @clean_handler
    def clean_and_storage(self):
        producer = DataProducer(id_queue=self._id_queue)
        while True:
            result = self.result_queue.get(block=False)
            producer.process_result(result)

    def run(self):
        self.generate_keyword()
        # self.keyword_queue.put('公司')
        self.crawl()
        self.clean_and_storage()
        self.generate_id()
        # while True:
        #     thread_names = [i.name for i in threading.enumerate()]
        #     # 当爬取和清洗完成之后结束阻塞主线程退出
        #     if 'clean' not in thread_names and 'spider' not in thread_names:
        #         break
        #     print(thread_names)
        #     time.sleep(5)


if __name__ == '__main__':
    worker = Worker()
    worker.run()
