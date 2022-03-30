# from ...monitoring import test_random_file
import json
import os
import sys
import time
from base64 import b64encode

import jsonschema
import pytest
import requests

from ...db_tools.rets import rets_class

rets_class=rets_class.rets_class()

from .config import env_vars, params
from ...aws.s3_aws import s3_class
from ...aws.sqs import sqs_class
# from ...gen_config import gen_params
# from ....db_tools.mongo_db import mongo_class
# from ....db_tools.postgress_db import postgress_class
# from ....e2e_editorial_gen.editorial_class import editorial_class
# from ....e2e_editorial_gen.features.check_results_class import check_results_class
from ...gen_features import set_environment

s3_class=s3_class.s3_class()
sqs_class=sqs_class.sqs_class()

# editorial=editorial_class()
# check_results=check_results_class()
# postgress_class=postgress_class.postgress_class()
# mongo_class=mongo_class.mongo_class()
# DB_WN=gen_params.DB_WN
# DB_GOLD=gen_params.DB_GOLD
# DB_MONGO=gen_params.DB_MONGO
root_dir=os.path.dirname(os.path.realpath(__file__))


def printTable(myDict, colList=None):
    """ Pretty print a list of dictionaries (myDict) as a dynamically sized table.
    If column names (colList) aren't specified, they will show in random order.
    Author: Thierry Husson - Use it as you want but don't blame me.
    """
    if not colList: colList=list(myDict[0].keys() if myDict else [])
    myList=[colList]  # 1st row = header
    for item in myDict: myList.append([str(item[col] if item[col] is not None else '') for col in colList])
    colSize=[max(map(len, col)) for col in zip(*myList)]
    formatStr=' | '.join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ['-' * i for i in colSize])  # Seperating line
    for item in myList: print(formatStr.format(*item))


def json_schema_validator(content, schema_path, logger):
    schema_dir=os.path.dirname(os.path.abspath(schema_path))

    lower_dict={}
    for key, val in content.items():
        lower_dict[key.lower()]=str(val).lower() if val and isinstance(val,
                                                                       str) else val
    # logger.info(" lower_dict: {}".format(lower_dict))
    with open(schema_path, 'r') as f:
        schema_data=f.read()
        # logger.info("schemaaaa {}".format(schema_data))
        schema=json.loads(schema_data)
        # logger.info("schemadir : {}".format(schema_dir))
        resolver=jsonschema.RefResolver(base_uri='file://' + schema_dir + '/', referrer=schema)
        logger.info('\n')
    try:
        # jsonschema.validate(lower_dict, schema, resolver=resolver)
        jsonschema.validate({
            "productId": 'rr',
            "productName": "A green door",
            "price": 12.50,
            "tags": ["home", "green"]
        }, {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "http://example.com/product.schema.json",
            "title": "Product",
            "description": "A product from Acme's catalog",
            "type": "object",
            "properties": {
                "productId": {
                    "description": "The unique identifier for a product",
                    "type": "integer"
                }
            },
            "required": ["productId"]
        })
        if ("mark" in schema_path):
            logger.critical("Passed mark filter json schema test")
        else:
            logger.critical("Passed basic filter json schema test")
    except Exception as e:
        parsed_log=repr(str(e).replace('\n', ', '))
        parsed_log=parsed_log.replace(', ,', ',')
        logger.info(parsed_log)


# def get_db_mongo_args(envin, collection='listings'):
#     db_mongo_args={
#         'dbname': DB_MONGO['listings']['dbname'][envin],
#         'collection': collection,
#         'cluster': DB_MONGO['listings']['cluster'][envin],
#         'user': DB_MONGO['user'],
#         'password': DB_MONGO['password'],
#         'auth': DB_MONGO['listings']['auth'][envin]
#     }
#     return db_mongo_args


# def check_listing(envin, logger, pid, vendor):
#     from ....aws.s3_aws import s3_class
#     s3_class=s3_class.s3_class()
#     day, month, year=env_vars.day, env_vars.month, env_vars.year
#     flag=False
#     path='{}/properties/{}/{}/{}'.format(vendor, year, month, day)
#     logger.info("The path where searching the file at is: {} ".format(path))
#     s3_files=s3_class.get_s3_files(envin, bucket='dev-mls-raw-data', prefix=path)
#     logger.info('\n')
#     logger.info("Getting the s3 files for the requested date")
#
#     for listing in range(len(s3_files)):
#         key_temp=s3_files[listing].key
#         response=s3_class.read_s3_file(envin, bucket='dev-mls-raw-data', prefix=key_temp)
#         logger.info("getting the response of {} file content".format(key_temp))
#         logger.info("length of the response: {}".format(len(response)))
#         # logger.info(response)
#         for record in range(len(response)):
#             property_id=response[record]['data']['property_id']
#             # logger.info("prop {}  {}".format(property_id,pid))
#             if str(pid) == str(property_id):
#                 logger.info('\n')
#                 logger.info("The record has been found in the s3")
#                 logger.info('\n')
#                 flag=True
#                 break
#         if flag:
#             break
#     return flag
#
#
# def verify_json_schema(pid, envin, logger, vendor):
#     logger.info("Starting the verification of the json schema")
#     records_per_mls_query="select row_to_json(t) from (select *  from normalized where property_id='{}') t".format(pid)
#     logger.info("the query that ran :{}".format(records_per_mls_query))
#     records_mls=postgress_class.get_query_psg(DB_GOLD[envin], records_per_mls_query)
#     if len(records_mls) == 0:  # The listing is not in the db
#         logger.info("The listing is not in the db ")
#     else:
#         count=0
#         for row in records_mls:
#             logger.info("\n")
#             # logger.info("Record {}: {} \n".format(count, row[0]))
#             # logger.info("typeee {}: {} \n".format(count, type(row[0])))
#             count+=1
#             schema_path='e2e/megatron/gen_config/schema_validator_basic_filter/{}_schema.json'.format(
#                 vendor.upper()[3:])
#             logger.info("Schema path is: {}".format(schema_path))
#             schema_dir=os.path.dirname(os.path.abspath(schema_path))
#             logger.info("Schemadir: {}".format(schema_dir))
#             json_schema_validator(row[0], schema_path, logger)
#
#             schema_path_mark='e2e/megatron/gen_config/schema_validator_mark_filter/{}_schema.json'.format(
#                 vendor.upper()[3:])
#             # schema_dir = os.path.dirname(os.path.abspath(schema_path_mark))
#             json_schema_validator(row[0], schema_path_mark, logger)
#
#
# def test_checking_listing_in_wn(envin, mls2id, vendor='all'):
#     set_environment.set_environment(envin, root_dir, env_vars)
#     logger=env_vars.logger
#     mls2id_list=mls2id.split(':')
#     vendor=mls2id_list[0]
#     pid=mls2id_list[1]
#     logger.info("vendor is {}".format(mls2id_list[0]))
#     mls=gen_params.vendors_map.get(mls2id_list[0])
#     logger.info("value in gen_paramaters {}".format(mls))
#
#     # Running the query with inner join
#     records_per_mls_query="select row_to_json(t) from (select pr.mls_update_date,listing_status  from {}_properties pr inner join {}_property_attributes pra on pr.property_id =pra.property_id where pr.property_id='{}') t".format(
#         mls, mls, pid)
#     # logger.info("the query that ran :{}".format(records_per_mls_query))
#     records_mls=postgress_class.get_query_psg(DB_WN, records_per_mls_query)
#     # logger.info("the inner join query results: {}".format(records_mls))
#
#     if len(records_mls) == 0:  # The listing is not in the db
#         records_per_mls_query="select row_to_json(t) from (select pr.mls_update_date  from {}_properties where pr.property_id='{}') t".format(
#             mls, pid)
#         # logger.info("the query that ran :{}".format(records_per_mls_query))
#         records_mls=postgress_class.get_query_psg(DB_WN, records_per_mls_query)
#         # logger.info("the inner join query results: {}".format(records_mls))
#         if len(records_mls) == 0:  # The listing is not in the db
#             logger.critical("Listing ID :{} isn't in the {} wn db ".format(pid, vendor))
#             assert False, "The listing is not in the DB"
#         else:
#             logger.warning("The listing is not in the property_attributes table")
#
#     else:  # the listing is in the db
#
#         mls_update_date=records_mls[0][0].get('mls_update_date')
#         env_vars.wn_listing_status=records_mls[0][0].get('listing_status')
#         if mls_update_date:
#             env_vars.mls_update_date=dateutil.parser.parse(mls_update_date)
#             # ls_epoc_time = datetime.datetime(ls.year, ls.month, ls.day, ls.hour, ls.minute, ls.second).timestamp()
#
#         env_vars.day=env_vars.mls_update_date.day
#         env_vars.month=env_vars.mls_update_date.month
#         env_vars.year=env_vars.mls_update_date.year
#         # logger.info("new: day {}  month {}   year {}".format(day,month,year))
#         env_vars.status=True
#         logger.info("The listing is in the WN MLS data")
#
#
# def test_checking_if_listing_is_in_s3_data(vendor, envin, mls2id):
#     set_environment.set_environment(envin, root_dir, env_vars)
#     logger=env_vars.logger
#     if (env_vars.status):
#         mls2id_list=mls2id.split(':')
#         vendor=mls2id_list[0]
#         pid=mls2id_list[1]
#         flag=check_listing(envin, logger, pid, vendor)
#         if not flag:
#             now=datetime.datetime.now()
#             env_vars.year=now.year
#             env_vars.month=now.month
#             env_vars.day=now.day
#
#             logger.info("Checking logs in the current date: {}\n".format(now))
#             flag=check_listing(envin, logger, pid, vendor)
#         if not flag:
#             logger.critical("Listing wasn't found in s3 file - Extractor/Retriever issue")
#             env_vars.status_is_in_s3=False
#         else:
#             env_vars.status_is_in_s3=True
#     else:
#         assert False, "Since object is not in DB test has stopped"
#
#
# def test_checking_if_listing_is_in_gold_DB(vendor, envin, mls2id):
#     set_environment.set_environment(envin, root_dir, env_vars)
#     # connecting to the Gold DB SQL and getting the query response of the relevant tables
#     if (env_vars.status_is_in_s3):
#         logger=env_vars.logger
#         mls2id_list=mls2id.split(':')
#         pid=mls2id_list[1]
#         vendor=mls2id_list[0]
#         # City white list verification agains the location_dict table
#         logger.info("Performing city white list verification")
#         records_per_mls_query="select n.city,n.property_id ,mls,l.is_enabled,mls_update_date from normalized n  inner join location_dict l on n.city =l.input_city where n.city in(select input_city from location_dict where is_enabled =false)"
#         logger.info("the query that ran :{}".format(records_per_mls_query))
#         records_mls=postgress_class.get_query_psg(DB_GOLD[envin], records_per_mls_query)
#         # logger.info("rrrrr {}".format(records_mls))
#         if len(records_mls) > 0:
#             logger.critical("property id: {} has disabled city".format(records_mls))
#         else:
#             logger.info("The white list city check passed successfully")
#
#         # mapping table
#         logger.info("Working on mapping table")
#         records_per_mls_query="select property_id, mls from mapped where property_id='{}'".format(pid)
#         logger.info("the query that ran :{}".format(records_per_mls_query))
#         records_mls=postgress_class.get_query_psg(DB_GOLD[envin], records_per_mls_query)
#         if len(records_mls) > 0:
#             logger.info("The record has been found in the mapping table")
#             logger.info("the query results : {} ".format(records_mls))
#         else:
#             logger.info("The record wasn't found in the mapping table")
#             assert False, "The record wasn't found in the mapping table"
#         logger.info('\n')
#
#         # normalized table
#         logger.info("Working on Normalized table")
#         records_per_mls_query="select property_id, mls from normalized where property_id ='{}'".format(pid)
#         logger.info("the query that ran :{}".format(records_per_mls_query))
#         records_mls=postgress_class.get_query_psg(DB_GOLD[envin], records_per_mls_query)
#         if len(records_mls) > 0:
#             logger.info("The record has been found in the normalized table")
#             logger.info("the query results : {} ".format(records_mls))
#             verify_json_schema(pid, envin, logger, vendor)
#         else:
#             logger.info("The record wasn't found in the normalized table")
#         logger.info('\n')
#
#         # metadata table
#         logger.info("Working on Meta-Data table")
#         records_per_mls_query="select property_id, mls, is_app_primary,mapper_timestamp,basic_filter_timestamp, normalizer_timestamp, mark_filter_timestamp, enrichment_timestamp,normalizer_version, enrichment_version, basic_filter_version, last_update_date,duplicate_type,is_app_primary from metadata_tracking where property_id ='{}'".format(
#             pid)
#         dict_of_columns={0: "property_id", 1: "mls", 2: "is_app_primary", 3: "mapper_timestamp",
#                          4: "basic_filter_timestamp", 5: "normalizer_timestamp", 6: "mark_filter_timestamp",
#                          7: "enrichment_timestamp", 8: "normalizer_version", 9: "enrichment_version",
#                          10: "basic_filter_version", 11: "last_update_date", 12: "duplicate_type", 13: "is_app_primary"}
#         logger.info("the query that ran in the meta-data :{}".format(records_per_mls_query))
#         records_mls=postgress_class.get_query_psg(DB_GOLD[envin], records_per_mls_query)
#         logger.info('\n')
#         if len(records_mls) > 0:
#             logger.info("the query results(meta data : {} ".format(records_mls))
#             logger.info("The record has been found in the metadata table and the records are:")
#             count=0
#             for row in records_mls:
#                 logger.info("\n")
#                 logger.info("Record {}: {} \n".format(count, row))
#                 count+=1
#                 None_flag=False
#                 for timestamp in range(3, 8):
#                     # logger.info("Timestamp is {}".format(timestamp))
#                     if (row[timestamp] == None):
#                         logger.error("Column {} is  None ".format(dict_of_columns[timestamp]))
#                         None_flag=True
#                 if (None_flag == False):
#                     if (row[6] >= row[5] >= row[4] >= row[3]):
#                         x='\n' + '\t' * 10
#                         logger.info(
#                             "The records timestamp are as expected:  {} 1. mapper_timestamp {}  2. basic_filter_timestamp {}  3.normalizer_timestamp{} 4. mark_filter_timestamp".format(
#                                 x, x, x, x))
#                     else:
#                         logger.info(
#                             "The records timestamp are not as expected: \n  1. mapper_timestamp\n  2. basic_filter_timestamp\n  3.normalizer_timestamp\n  4. mark_filter_timestamp")
#
#                 # Checking that the versions aren't empty
#                 logger.info('\n')
#                 for version in range(8, 11):
#                     if (row[version] == None):
#                         logger.info("column number(version) {} is None".format(dict_of_columns[version]))
#
#                 # Checking that the last_update_that >= mls_update_date
#                 try:
#                     ttt=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[11] / 1000))
#                     ttt=datetime.datetime.strptime(ttt, '%Y-%m-%d %H:%M:%S')
#                     logger.info(
#                         " dates that are being tested 1. the last update date: {} 2. The mls update date:{}".format(ttt,
#                                                                                                                     env_vars.mls_update_date))
#                     if (ttt <= env_vars.mls_update_date):
#                         logger.error("The last_update_that < mls_update_date ")
#                     else:
#                         logger.info("The last update date is valid")
#                 except:
#                     logger.error("There was an error with the last update date column")
#
#         else:
#             logger.info("The record wasn't found in the metadata table")
#             env_vars.status_is_in_mapped=False
#     else:
#         assert False, "The record is not in s3"
#
#
# def test_checking_if_listing_is_in_mongo(vendor, envin, mls2id):
#     logger=env_vars.logger
#     # Connectiong to Mongo DB and finding the listing
#     external_id_2_listing_id={}
#     # used_ids = list(env_vars.used_ids.keys())
#     logger=env_vars.logger
#     mls2id_list=mls2id.split(':')
#     pid=mls2id_list[1]
#     env_vars.used_ids=[pid]
#     used_ids=env_vars.used_ids
#     listings_query={"currentListing.vendorMlsId": {"$in": used_ids}}
#     print('quering mongo {} with: {}'.format(envin, listings_query))
#     db_mongo_args=get_db_mongo_args(envin, 'listings')
#     mong_resp=mongo_class.get_find_query_mongo(db_mongo_args, listings_query)
#     logger.info('\n')
#     if (len(mong_resp) > 0):
#         logger.info("The listing has been found in the mongo db")
#         for resp in mong_resp:
#             external_id_2_listing_id[resp.get('currentListing').get('listingExternalId')]=resp.get(
#                 'currentListing').get(
#                 'vendorMlsId')
#             logger.info(external_id_2_listing_id)
#     else:
#         assert False, "This listing is not in the app DB"


import datetime


# borrowed from https://stackoverflow.com/a/13565185
# as noted there, the calendar module has a function of its own
def last_day_of_month(any_day):
    next_month=any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


begin="2018-02-15"
end="2018-04-23"


def monthlist(begin, end):
    date_patern="%Y-%m-%dT%H:%M:%S"
    begin=datetime.datetime.strptime(begin, date_patern)
    end=datetime.datetime.strptime(end, date_patern)

    result=[]
    while True:
        if begin.month == 12:
            next_month=begin.replace(year=begin.year + 1, month=1, day=1)
        else:
            next_month=begin.replace(month=begin.month + 1, day=1)
        if next_month > end:
            break
        result.append([begin.strftime(date_patern), last_day_of_month(begin).strftime(date_patern)])
        begin=next_month
    result.append([begin.strftime(date_patern), end.strftime(date_patern)])
    return result


def startLogger(logPath, mode='w'):
    import logging
    with open(logPath, 'w') as log:
        'just delete log'
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler=logging.FileHandler(logPath)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the env_varss to the logger
    logger.addHandler(handler)
    return logger


def get_set_secrerts(envin):
    db_mongo={}
    db_gold={}
    db_wn={}
    db_rets={}
    try:
        services=['de-extractor', 'de-saver', 'comp-proxy']
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf=s3_class.get_secrets_by_service(envin, service_name, params)
                # conf, secret_name, region_name=s3_class.get_secrets_by_service(envin, service_name, params)
                # if not conf:
                #     env_vars.logger.critical('Could not get configuration from secret={} at region={}'.format(secret_name,region_name))
                #     raise SystemExit()
                env_vars.aws_secrets[service_name]=conf

        db_gold[envin]=set_postgress_db_from_secret_jdbc('de-saver', db_source='rds')
        db_wn=set_postgress_db_from_secret_jdbc('de-extractor', db_source='wolfnet')
        db_mongo=env_vars.aws_secrets['comp-proxy'].get('mongodb').get('uri')
        db_rets=env_vars.aws_secrets['de-extractor'].get('rets')
    except:
        info=sys.exc_info()
        print(info)

    return db_mongo, db_gold, db_wn, db_rets


def set_postgress_db_from_secret_jdbc(service_name, db_source):
    db_secrets={}

    host=None
    port=None
    dbname=None

    con_str=env_vars.aws_secrets.get(service_name).get('db').get(db_source).get('url')
    con_str_elements=con_str.split('/')
    for ce in con_str_elements:
        if 'com:' in ce:
            ce_host_port_list=ce.split(':')
            host=ce_host_port_list[0]
            port=ce_host_port_list[1]
        elif '?' in ce:
            dbname_list=ce.split('?')
            dbname=dbname_list[0]

    if host and port and dbname and port.isnumeric:
        port=int(port)

        db_secrets={
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': env_vars.aws_secrets.get(service_name).get('db').get(db_source).get('user'),
            'password': env_vars.aws_secrets.get(service_name).get('db').get(db_source).get('password'),
            'collection': env_vars.aws_secrets.get(service_name).get('db').get(db_source).get('schema')
        }

    return db_secrets

def direct_photos_res_stream(vendor, rets_args=None, on_market=True):

    global UPDATE_SINCE_RETS, UPDATE_TILL_RETS
    if not rets_args:
        rets_args=DB_RETS.get(vendor)
    rets_photos_per_id={}
    try:

        date_list=monthlist(UPDATE_SINCE_RETS, UPDATE_TILL_RETS)

        sandicor_property_type=["RE_1", "RI_2"]

        if vendor == 'sandicor':
            off_market_status_str=rets_class.get_sandicor_status_str()

        rets_version=rets_args.get('rets-version')
        header_cookie=None

        for p_type in sandicor_property_type:
            for dtl in date_list:
                from_date=dtl[0]
                to_date=dtl[1]

                # Count= 0 - for listings only; 1 - for listings and count, 2 - for count only
                if on_market:
                    off_market_sign='~'
                else:
                    off_market_sign=''

                query_photos="(MED_update_dt={}-{}),{}(L_Status={})".format(from_date, to_date,
                                                                            off_market_sign,
                                                                            off_market_status_str)
                #query_photos='(MED_update_dt=2017-01-01T00:00:00-2020-08-04T00:00:00),~(L_Status=2_0,4_0,5_0,5_1,5_2,5_3,6_0)'
                query_photos_select="L_DisplayId"
                query_photos_full="SearchType=Media&Class={}&" \
                                  "Query={}&Select={}&" \
                                  "RETS-Version={}&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1".format(
                    p_type,
                    query_photos,
                    query_photos_select, rets_version)

                query=query_photos_full

                req_type='search'

                rets_res, header_cookie=rets_class.get_query_rets_stream(rets_args, req_type, query, header_cookie)

                root = ET.fromstring(rets_res.content)
                columns_list=[]
                columns_vals=[]
                vals_records=[]
                for c in root.iter():
                    key = c.tag
                    val = c.text
                    if val:
                        val=val[1:len(val)-1]
                    if key == 'COUNT':
                        print('number of results = {}'.format(c.attrib.get('Records')))
                    if key == 'COLUMNS':
                        columns_list=val.split('\t')

                    if key == 'DATA':
                        columns_vals = val.split('\t')

                    if len(columns_list)!=0 and len(columns_vals) == len(columns_list):
                        listing_id =columns_vals[0]

                        if not rets_photos_per_id.get(listing_id):
                            rets_photos_per_id[listing_id]=1
                        else:
                            rets_photos_per_id[listing_id]+=1
                print('done')


                # import xml.etree.ElementTree as ET
                # for elem in ET.iterparse(rets_res.raw):
                #     process(element)
                # if rets_res.content:
                #     events=ElementTree.iterparse(rets_res.raw)
                #     for event, elem in events:
                # for chunk in rets_res.iter_content(chunk_size=10240):
                #
                #     rets_data, columns_list=rets_class.rets_xml_to_list_res_stream(chunk)
                #
                #     if rets_data:
                #         for d in rets_data:
                #             d_list=d.split('\t')
                #             if len(d_list) > 0 and d_list[0].isnumeric():
                #                 listing_id=d_list[0]
                #                 if not rets_photos_per_id.get(listing_id):
                #                     rets_photos_per_id[listing_id]=1
                #                 else:
                #                     rets_photos_per_id[listing_id]+=1



    except:
        info=sys.exc_info()
        print(info)
    return rets_photos_per_id


@pytest.mark.test_direct_alignment_new
def test_direct_alignment_new(envin, aws_new):
    import xmltodict
    try:
        if str(aws_new).lower() != 'true':
            aws_new=False
        params.aws_new_account=aws_new
        print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
        global DB_MONGO, DB_GOLD, DB_WN, DB_RETS

        env_vars.summary={}
        env_vars.mis_lis=[]
        env_vars.plus_lis=[]
        env_vars.duplicate_count=0
        env_vars.postg_res_count=0
        env_vars.mis_city=[]
        env_vars.env=envin
        start=time.time()
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        DB_MONGO, DB_GOLD, DB_WN, DB_RETS=get_set_secrerts(envin)

        vendor = 'sandicor'
        rets_args=DB_RETS.get(vendor)
        mls_res_dict=direct_photos_res_stream(vendor, rets_args, on_market=True)

        on_market=True
        begin='2020-01-01T00:00:00'
        end='2020-07-23T00:00:00'

        date_list=monthlist(begin, end)
        sandicor_property_type=["RE_1", "RI_2"]
        rets_args=DB_RETS.get('sandicor')
        rets_data_list=[]

        sandicor_status_map={
            "SOLD": "2_0",
            "EXPIRED": "4_0",
            "WITHDRAWN": "5_0",
            "CANCELLED": "5_1",
            "HOLD": "5_2",
            "DELETED": "5_3",
            "RENTED": "6_0"
        }
        off_market_status_str=''

        for key, val in sandicor_status_map.items():
            off_market_status_str+=val + ','

        if off_market_status_str.endswith(','):
            off_market_status_str=off_market_status_str[:-1]
        header_cookie=None
        for p_type in sandicor_property_type:
            # for dtl in date_list:
            #     from_date=dtl[0]
            #     to_date=dtl[1]

            # Count= 0 - for listings only; 1 - for listings and count, 2 - for count only
            # extractor update query = %28L_UpdateDate%3D2020-07-27T11%3A15%3A37Z-2020-07-24T11%3A15%3A37Z%29
            if on_market:
                off_market_sign='~'
            else:
                off_market_sign=''

            # query_photos ="SearchType=Media&Class=RE_1&" \
            #               "Query=(L_DisplayId=200030319),~(%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%28MED_update_dt%3D2020-05-29T23%3A18%3A47Z-2020-05-15T22%3A18%3A47Z%29)&Select=L_DisplayId&" \
            #               "RETS-Version=RETS%2F1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1"
            # query= query_photos

            listing_ids="200030319,200028812,200029758,200022429,200028187,200032718,200032289,200029840,200010782,200028530,200027430,200027928,200031274,200021876,200019614,200019759,200018689,200030541,200030573,200030909,200004854,200005168,200017559,200028236,200028811,200025721,200032311,200021344,200027015,200027892,200027315,200028415,200025655,200007878,200029630,200030109,200029741,200032848,200029610,200018573,200017687,200026901,200000316,200020064,200020903,200032954,200030893,200031917,200030104,200025383,200019549,200030915,200030986,200027782,200026759,200028839,200021225,200030411,200018849,200031397,200011047,200024002,200025994,200025553,200014347,200031602,200030667,200022566,200029649,200014168,200024075,200033742,200031073,200029473,200032060,200026156,200030300,200000706,200001837,200030215,200021918,200027713,200001168,200028701,200025564,200027709,200031054,200027648,200028819,200009713,200027521,200025497,200029329,200026234,200030281,200021794,200027196,200032308,200024474,200020906,200005515,190054254,200024798,200023295,200020416,200027104,200014304,200005431,200027931,200029655,200029479,200034112,190045135,200029648,200028696,200032993,200025759,200029134,200021224,200027080,200031374,200029186,200031725,200024355,200026034,200018155,200021868,200012977,200029283,200031452,200006076,200028200,200030124,200031012,200025744,200020439,200029595,200031049,200021168,200002874,200030302,200026563,200019296,190066220,200021823,200029471,200026672,200021752,200013233,200029129,200031024,200018322,200014337,200028060,200029751,200028296,200029617,200032255,190054016,200028218,200011252,200018389,200033174,200030061,200030337,200031409,200028578,200028522,200030331,200029378,200015958,200029040,200024266,200031111,200032338,200018113,200023481,200030493,200030804,200024155,200029293,200027689,200031468,200026640,200008333,190033617,200023498,200024493,190052897,200031843,200030670,200029594,200032738,200005135,200026730,200032856,200030537,200000979,200032050,200033991,200031101,200026086,200030583,200019196,200023277,200032097,200029016,200031634,200029045,200024807,200027059,200025622,200030137,200015629,200029060,200012766,200011717,200017042,200030003,200029059,200015706,200031238,200030768,200029052,200014326,200026399,200021973,200023520,200006131,200029417,200028331,200026348,200026433,200027837,200019351,200028332,200025629,200029761,200026893,200029152,200030580,200032272,200029107,200029122,200031524,200027960,200029049,200029111,200024529,200025458,200030552,200029086,200033208,200031144,200024789,200009929,200027730,200031574,200029549,200027871,200019102,200026914,200030673,200032919,200030810,200028535,200031561,200006006,200022755,200031551,200031179,200025550,200032092,200029836,200032817,200029374,200005267,200029749,200029552,200004785,200027614,200009293,200025968,200029226,200027743,200016336,200017122,200024921,200014850,200028579,200014509,200028514,200033902,200033111,200009080,200016631,200021833,200019167,200029477,200030542,200033094,200027270,200029492,200008057,200023425,200029955,200027778,200010462,200026745,200031533,200006883,200027593,200032652,200028385,200020119,200026173,200034038,200031192,200027527,200031708,200027618,200031399,200013634,200030599,200030696,200026655,200029587,200032703,200024262,200024210,200031252,200030345,200023882,200028216,200030336,200031269,200022493,200031326,200009277,200023955,200029879,200027796,200012928,200027447,200028405,200028220,200006387,200026432,200026641,200027076,200033915,200028758,200012553,200026569,200024292,200028347,200031170,200026078,200031199,200029215,200022213,200008760,200023007,200019124,190019056,200025607,200022608,200028844,200025667,200017367,200024346,200025182,200012092,200031253,200028996,200026039,200028469,200026878,200030416,200030706,190064070,200027324,200025854,200012923,200027318,200004728,200031740,200019446,200027091,200033694,200026968,200031463,200009161,200030566,200018709,200026767,200031541,200030847,200026669,190045459,190058988,200022867,200028708,200028543,190056438,200016697,200002005,200023967,200020590,200026992,200018932,200027327,200027317,200017343,200022972,200019801,200025641,200024641,200031362,200019467,200031521,200016707,200028257,200031053,190057678,200017164,200018993,200026372,200030998,200028960,200030891,200029442,200031497,200027208,200022698,200021109,200029187,200028588,200005194,200030962,200014199,200030669,200014220,200028263,200026738,200031766,200028455,200028484,200013147,200028472,200027230,200029085,200027941,200028634,200028648,200029177,190044569,200028321,200014828,200025404,200024323,200028772,200027029,200026422,200028488,200018593,200025644,200011664,200021144,200030689,200022640,200030961,200029348,200022710,200021676,200012810,190040096,200026884,200010016,200018921,190047194,200015061,200005991,200011434,200008984,200005624,200028911,200030755,200012000,200027577,190059818,200008914,200017295,200020811,200003860,200001769,200005878,200021078,200007593,200005998"

            query_photos="(L_DisplayId={}),~%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%29".format(
                listing_ids)

            t="%28MED_update_dt%3D2020-01-01T23%3A18%3A47Z-2020-05-30T22%3A18%3A47Z%29"
            query_photos="{},~%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%29".format(t)
            query_photos_select="L_DisplayId,LO1_board_id"
            query_photos_full="SearchType=Media&Class={}&" \
                              "Query={}&Select={}&" \
                              "RETS-Version=RETS%2F1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=2".format(p_type,
                                                                                                                query_photos,
                                                                                                                query_photos_select)

            query_photos_full="Format=COMPACT-DECODED&" \
                              "Class={}&" \
                              "Select={}&" \
                              "RETS-Version=RETS%2F1.7.2&" \
                              "QueryType=DMQL2&" \
                              "Query={}&" \
                              "SearchType=Media&" \
                              "Count=2".format(p_type, query_photos_select, query_photos)
            query=query_photos_full

            # query_properties = "(LO1_board_id=1),(L_UpdateDate={}-{}),{}(L_Status={})".format(from_date, to_date, off_market_sign, off_market_status_str)
            # query_properties_select = "LM_Char10_1,L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id"
            #
            # query_properties_full="SearchType=Property&Class={}&" \
            #                       "Query={}&" \
            #                       "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
            #                       "Select={}".format(
            #     p_type, query_properties, query_properties_select)

            # query_properties_full="SearchType=Property&Class={}&" \
            #                  "Query=(LO1_board_id=1),(L_UpdateDate={}-{}),{}(L_Status={})&" \
            #                  "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
            #                  "Select=LM_Char10_1,L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
            #     p_type, from_date, to_date, off_market_sign, off_market_status_str)

            # query=query_properties_full

            # query_on_market="SearchType=Property&Class={}&" \
            #                 "Query=(L_UpdateDate={}-{}),~(L_Status={}),(LO1_board_id=San Diego Association of Realtors)&" \
            #                 "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
            #                 "Select=L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
            #     p_type, from_date, to_date, off_market_status_str)
            #
            # query_off_market="SearchType=Property&Class={}&" \
            #                  "Query=(L_UpdateDate={}-{}),(L_Status={}),(LO1_board_id=San Diego Association of Realtors)&" \
            #                  "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
            #                  "Select=L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
            #     p_type, from_date, to_date, off_market_status_str)
            #
            # if on_market:
            #     query = query_on_market
            # else:
            #     query = query_off_market

            req_type='search'
            rets_args=DB_RETS.get('sandicor')
            rets_res, header_cookie=rets_class.get_query_rets_stream(rets_args, req_type, query, header_cookie)
            #rets_res, header_cookie=rets_class.get_query_rets(rets_args, req_type, query, header_cookie)
            my_json=rets_res.content.decode('utf8').replace("'", '"')
            rets_dict=xmltodict.parse(my_json)
            rets_columns=None
            if rets_dict.get('RETS'):
                rets_columns=rets_dict.get('RETS').get('COLUMNS')

            if rets_columns:
                columns_list=rets_columns.split('\t')
                rets_data=rets_dict.get('RETS').get('DATA')
                if isinstance(rets_data, str):
                    rets_data=[rets_data]

                for d in rets_data:
                    my_dict={}
                    d_list=d.split('\t')
                    for i in range(len(columns_list)):
                        my_dict[columns_list[i]]=d_list[i]
                    rets_data_list.append(my_dict)
            rets_photos_per_id={}
            for rdl in rets_data_list:
                lid=rdl.get('L_DisplayId')
                if not rets_photos_per_id.get(lid):
                    rets_photos_per_id[lid]=1
                else:
                    rets_photos_per_id[lid]+=1

        for p_type in sandicor_property_type:
            for dtl in date_list:
                from_date=dtl[0]
                to_date=dtl[1]

                # Count= 0 - for listings only; 1 - for listings and count, 2 - for count only
                # extractor update query = %28L_UpdateDate%3D2020-07-27T11%3A15%3A37Z-2020-07-24T11%3A15%3A37Z%29
                if on_market:
                    off_market_sign='~'
                else:
                    off_market_sign=''

                # query_photos ="SearchType=Media&Class=RE_1&" \
                #               "Query=(L_DisplayId=200030319),~(%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%28MED_update_dt%3D2020-05-29T23%3A18%3A47Z-2020-05-15T22%3A18%3A47Z%29)&Select=L_DisplayId&" \
                #               "RETS-Version=RETS%2F1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1"
                # query= query_photos

                # listing_ids="200030319,200028812,200029758,200022429,200028187,200032718,200032289,200029840,200010782,200028530,200027430,200027928,200031274,200021876,200019614,200019759,200018689,200030541,200030573,200030909,200004854,200005168,200017559,200028236,200028811,200025721,200032311,200021344,200027015,200027892,200027315,200028415,200025655,200007878,200029630,200030109,200029741,200032848,200029610,200018573,200017687,200026901,200000316,200020064,200020903,200032954,200030893,200031917,200030104,200025383,200019549,200030915,200030986,200027782,200026759,200028839,200021225,200030411,200018849,200031397,200011047,200024002,200025994,200025553,200014347,200031602,200030667,200022566,200029649,200014168,200024075,200033742,200031073,200029473,200032060,200026156,200030300,200000706,200001837,200030215,200021918,200027713,200001168,200028701,200025564,200027709,200031054,200027648,200028819,200009713,200027521,200025497,200029329,200026234,200030281,200021794,200027196,200032308,200024474,200020906,200005515,190054254,200024798,200023295,200020416,200027104,200014304,200005431,200027931,200029655,200029479,200034112,190045135,200029648,200028696,200032993,200025759,200029134,200021224,200027080,200031374,200029186,200031725,200024355,200026034,200018155,200021868,200012977,200029283,200031452,200006076,200028200,200030124,200031012,200025744,200020439,200029595,200031049,200021168,200002874,200030302,200026563,200019296,190066220,200021823,200029471,200026672,200021752,200013233,200029129,200031024,200018322,200014337,200028060,200029751,200028296,200029617,200032255,190054016,200028218,200011252,200018389,200033174,200030061,200030337,200031409,200028578,200028522,200030331,200029378,200015958,200029040,200024266,200031111,200032338,200018113,200023481,200030493,200030804,200024155,200029293,200027689,200031468,200026640,200008333,190033617,200023498,200024493,190052897,200031843,200030670,200029594,200032738,200005135,200026730,200032856,200030537,200000979,200032050,200033991,200031101,200026086,200030583,200019196,200023277,200032097,200029016,200031634,200029045,200024807,200027059,200025622,200030137,200015629,200029060,200012766,200011717,200017042,200030003,200029059,200015706,200031238,200030768,200029052,200014326,200026399,200021973,200023520,200006131,200029417,200028331,200026348,200026433,200027837,200019351,200028332,200025629,200029761,200026893,200029152,200030580,200032272,200029107,200029122,200031524,200027960,200029049,200029111,200024529,200025458,200030552,200029086,200033208,200031144,200024789,200009929,200027730,200031574,200029549,200027871,200019102,200026914,200030673,200032919,200030810,200028535,200031561,200006006,200022755,200031551,200031179,200025550,200032092,200029836,200032817,200029374,200005267,200029749,200029552,200004785,200027614,200009293,200025968,200029226,200027743,200016336,200017122,200024921,200014850,200028579,200014509,200028514,200033902,200033111,200009080,200016631,200021833,200019167,200029477,200030542,200033094,200027270,200029492,200008057,200023425,200029955,200027778,200010462,200026745,200031533,200006883,200027593,200032652,200028385,200020119,200026173,200034038,200031192,200027527,200031708,200027618,200031399,200013634,200030599,200030696,200026655,200029587,200032703,200024262,200024210,200031252,200030345,200023882,200028216,200030336,200031269,200022493,200031326,200009277,200023955,200029879,200027796,200012928,200027447,200028405,200028220,200006387,200026432,200026641,200027076,200033915,200028758,200012553,200026569,200024292,200028347,200031170,200026078,200031199,200029215,200022213,200008760,200023007,200019124,190019056,200025607,200022608,200028844,200025667,200017367,200024346,200025182,200012092,200031253,200028996,200026039,200028469,200026878,200030416,200030706,190064070,200027324,200025854,200012923,200027318,200004728,200031740,200019446,200027091,200033694,200026968,200031463,200009161,200030566,200018709,200026767,200031541,200030847,200026669,190045459,190058988,200022867,200028708,200028543,190056438,200016697,200002005,200023967,200020590,200026992,200018932,200027327,200027317,200017343,200022972,200019801,200025641,200024641,200031362,200019467,200031521,200016707,200028257,200031053,190057678,200017164,200018993,200026372,200030998,200028960,200030891,200029442,200031497,200027208,200022698,200021109,200029187,200028588,200005194,200030962,200014199,200030669,200014220,200028263,200026738,200031766,200028455,200028484,200013147,200028472,200027230,200029085,200027941,200028634,200028648,200029177,190044569,200028321,200014828,200025404,200024323,200028772,200027029,200026422,200028488,200018593,200025644,200011664,200021144,200030689,200022640,200030961,200029348,200022710,200021676,200012810,190040096,200026884,200010016,200018921,190047194,200015061,200005991,200011434,200008984,200005624,200028911,200030755,200012000,200027577,190059818,200008914,200017295,200020811,200003860,200001769,200005878,200021078,200007593,200005998"
                #
                # query_photos="(L_DisplayId={}),~%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%29".format(
                #     listing_ids)
                # query_photos_select="L_DisplayId"
                # query_photos_full="SearchType=Media&Class={}&" \
                #                   "Query={}&Select={}&" \
                #                   "RETS-Version=RETS%2F1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1".format(
                #     p_type, query_photos, query_photos_select)
                #
                # query=query_photos_full

                query_properties = "(L_UpdateDate={}-{}),{}(L_Status={})".format(from_date, to_date, off_market_sign, off_market_status_str)
                query_properties_select = "LM_Char10_1,L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id"
                #
                query_properties_full="SearchType=Property&Class={}&" \
                                      "Query={}&" \
                                      "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
                                      "Select={}".format(
                    p_type, query_properties, query_properties_select)

                # query_properties_full="SearchType=Property&Class={}&" \
                #                  "Query=(LO1_board_id=1),(L_UpdateDate={}-{}),{}(L_Status={})&" \
                #                  "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
                #                  "Select=LM_Char10_1,L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
                #     p_type, from_date, to_date, off_market_sign, off_market_status_str)

                # query=query_properties_full

                # query_on_market="SearchType=Property&Class={}&" \
                #                 "Query=(L_UpdateDate={}-{}),~(L_Status={}),(LO1_board_id=San Diego Association of Realtors)&" \
                #                 "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
                #                 "Select=L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
                #     p_type, from_date, to_date, off_market_status_str)
                #
                # query_off_market="SearchType=Property&Class={}&" \
                #                  "Query=(L_UpdateDate={}-{}),(L_Status={}),(LO1_board_id=San Diego Association of Realtors)&" \
                #                  "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
                #                  "Select=L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
                #     p_type, from_date, to_date, off_market_status_str)
                #
                # if on_market:
                #     query = query_on_market
                # else:
                #     query = query_off_market

                query = query_properties_full
                req_type='search'
                rets_res, header_cookie=rets_class.get_query_rets(rets_args, req_type, query, header_cookie)
                my_json=rets_res.content.decode('utf8').replace("'", '"')
                rets_dict=xmltodict.parse(my_json)
                rets_columns=None
                if rets_dict.get('RETS'):
                    rets_columns=rets_dict.get('RETS').get('COLUMNS')

                if rets_columns:
                    columns_list=rets_columns.split('\t')
                    rets_data=rets_dict.get('RETS').get('DATA')
                    if isinstance(rets_data, str):
                        rets_data=[rets_data]

                    for d in rets_data:
                        my_dict={}
                        d_list=d.split('\t')
                        for i in range(len(columns_list)):
                            my_dict[columns_list[i]]=d_list[i]
                        rets_data_list.append(my_dict)

        off_market_list=[]
        on_market_list=[]
        valid_active={}
        cnt_broken_check={'county': [], 'originating_system_name': [], 'listing_price': []}
        for dl in rets_data_list:
            listing_status=dl.get('L_Status')
            listing_price=dl.get('L_AskingPrice')
            listing_id=dl.get('L_ListingID')
            listing_city=dl.get('L_City')
            addreess1=dl.get('L_AddressNumber')
            addreess2=dl.get('L_AddressSearchNumber')
            county=dl.get('LM_Char10_1')
            originating_system_name=dl.get('LM_char10_75')

            if on_market:
                on_market_list.append(listing_id)
            else:
                off_market_list.append(listing_id)

            if county != 'San Diego':
                cnt_broken_check['county'].append(listing_id)
            if originating_system_name != 'Sandicor':
                cnt_broken_check['originating_system_name'].append(listing_id)
            if not listing_price:
                cnt_broken_check['listing_price'].append(listing_id)
            if listing_price and int(listing_price) < 10000:
                cnt_broken_check['listing_price'].append(listing_id)

            if listing_price and int(listing_price) >= 30000 and (addreess1 != '' or addreess2 != ''):
                valid_active[listing_id]=listing_city

        printTable(rets_data_list)
        print('On_market_list:\n{}\n', format(on_market_list))
        print('Off_market_list:\n{}\n', format(off_market_list))
        print('Valid_Active_list:\n{}\n', format(valid_active))
    except:
        info=sys.exc_info()
        print(info)
    print('Elad')





@pytest.mark.test_direct_alignment
def test_direct_alignment1(envin, aws_new):
    import xmltodict
    try:
        params.aws_new_account=aws_new
        print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
        global DB_MONGO, DB_GOLD, DB_WN, DB_RETS

        env_vars.summary={}
        env_vars.mis_lis=[]
        env_vars.plus_lis=[]
        env_vars.duplicate_count=0
        env_vars.postg_res_count=0
        env_vars.mis_city=[]
        env_vars.env=envin
        start=time.time()
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        DB_MONGO, DB_GOLD, DB_WN, DB_RETS=get_set_secrerts(envin)
        res=False
        res_acc=[]
        dup_res={}

        # envin='prod'
        set_environment.set_environment(envin, root_dir, env_vars)
        logger=env_vars.logger

        vendor='sandicor'
        header_cookie=''
        sandicor_status_map={
            "SOLD": "2_0",
            "EXPIRED": "4_0",
            "WITHDRAWN": "5_0",
            "CANCELLED": "5_1",
            "HOLD": "5_2",
            "DELETED": "5_3",
            "RENTED": "6_0"
        }
        off_market_status_str=''

        for key, val in sandicor_status_map.items():
            off_market_status_str+=val + ','

        if off_market_status_str.endswith(','):
            off_market_status_str=off_market_status_str[:-1]

        sandicor_property_type=["RE_1", "RI_2"]
        sandicor_db_dict=DB_RETS.get('sandicor')
        base_url=sandicor_db_dict.get('base-url')
        rets_version=sandicor_db_dict.get('rets-version')
        user=sandicor_db_dict.get('user')
        password=sandicor_db_dict.get('password')

        userAndPass=b64encode(str.encode("{}:{}".format(user, password))).decode()
        request_type='login'
        login_url='{}/{}?{}'.format(base_url, request_type, rets_version)
        # login_url='https://sdmls-rets.connectmls.com/server/login?RETS-Version=RETS/1.7.2'
        headers={'Authorization': 'Basic {}'.format(userAndPass)}

        res1=requests.get(login_url, headers=headers)
        print("response of  request : {}".format(res1.content))

        set_cookie=res1.headers._store.get('set-cookie')
        if set_cookie and len(set_cookie) > 1:
            cookie_info=set_cookie[1]
            cookie_info_list=cookie_info.split(';')[0]
            session_data_list=cookie_info_list.split(',')
            cookie_session=session_data_list[0]
            http_sesion=session_data_list[1]
            header_cookie='{}; {}'.format(cookie_session, http_sesion)



        search_url='https://sdmls-rets.connectmls.com/server/search?'


        begin='2020-01-01T00:00:00'
        end='2020-07-23T00:00:00'

        date_list=monthlist(begin, end)
        print(date_list)

        data_list=[]

        search_url_query="https://sdmls-rets.connectmls.com/server/search?" \
                         "Format=COMPACT-DECODED&Class=RI_2&" \
                         "Select=L_DisplayId,L_Status,MED_media_url,MED_update_dt&" \
                         "Limit=100&RETS-Version=RETS%2F1.7.2&" \
                         "QueryType=DMQL2&" \
                         "Query=~%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%28" \
                         "MED_update_dt%3D2020-05-29T23%3A18%3A47Z-2020-05-15T22%3A18%3A47Z%29&" \
                         "SearchType=Media&Count=1"

        search_url_query="https://sdmls-rets.connectmls.com/server/search?" \
                         "Format=COMPACT-DECODED&Class=RI_2&" \
                         "Select=L_DisplayId,L_Status,MED_media_url,MED_update_dt&" \
                         "RETS-Version=RETS%2F1.7.2&" \
                         "QueryType=DMQL2&" \
                         "Query=~%28L_Status%3D5_3%2C5_0%2C2_0%2C4_0%2C5_1%2C5_2%2C6_0%29%2C%28" \
                         "MED_update_dt%3D2020-05-29T23%3A18%3A47Z-2020-05-15T22%3A18%3A47Z%29&" \
                         "SearchType=Media&Count=1".format(off_market_status_str)

        # "%28MED_update_dt%3D2017-01-01T00:00:00-2017-01-02T00:00:00%29&" \
        payload={}
        photos_headers={
            'Authorization': 'Basic NTAxMDgzOkNQNU1id2Zw',
            'User': '501083,1,23,862114',
            'Broker': '166137,166137',
            'MemberName': 'Roger Cummings',
            'Cookie': header_cookie
        }

        r=requests.request("GET", search_url_query, headers=photos_headers, data=payload, stream=True)

        my_json=r.content.decode('utf8').replace("'", '"')
        o=xmltodict.parse(my_json)
        # print("xxx {}".format(o))
        res_columns=o.get('RETS').get('COLUMNS')

        if r.encoding is None:
            r.encoding='utf-8'

        for line in r.iter_lines(decode_unicode=True):
            # my_json=line.decode('utf8').replace("'", '"')
            # o=xmltodict.parse(my_json)
            # # print("xxx {}".format(o))
            # res_columns=o.get('RETS').get('COLUMNS')
            # if res_columns:
            #     columns_list=res_columns.split('\t')
            #     res_data=o.get('RETS').get('DATA')
            #     if isinstance(res_data, str):
            #         res_data=[res_data]
            #
            #     for d in res_data:
            #         my_dict={}
            #         d_list=d.split('\t')
            #         for i in range(len(columns_list)):
            #             my_dict[columns_list[i]]=d_list[i]
            #         data_list.append(my_dict)
            if line:
                print(line)

        properties_headers={
            'Authorization': 'Basic NTAxMDgzOkNQNU1id2Zw',
            'Cookie': header_cookie
        }

        for p_type in sandicor_property_type:
            for dtl in date_list:

                query="SearchType=Property&Class={}&" \
                      "Query=(L_UpdateDate={}-{}),~(L_Status={})&" \
                      "RETS-Version=RETS/1.7.2&QueryType=DMQL2&Format=COMPACT-DECODED&Count=1&" \
                      "Select=L_AddressNumber,L_ListingID,L_City,L_Status,L_UpdateDate,L_AskingPrice,LM_char10_75,LO1_board_id".format(
                    p_type,
                    dtl[0], dtl[1], off_market_status_str)

                search_url_query=search_url + query
                payload={}

                res2=requests.request("GET", search_url_query, headers=properties_headers, data=payload)

                my_json=res2.content.decode('utf8').replace("'", '"')
                o=xmltodict.parse(my_json)
                # print("xxx {}".format(o))
                res_columns=o.get('RETS').get('COLUMNS')

                if res_columns:
                    columns_list=res_columns.split('\t')
                    res_data=o.get('RETS').get('DATA')
                    if isinstance(res_data, str):
                        res_data=[res_data]

                    for d in res_data:
                        my_dict={}
                        d_list=d.split('\t')
                        for i in range(len(columns_list)):
                            my_dict[columns_list[i]]=d_list[i]
                        data_list.append(my_dict)

                    # pd.set_option('display.width', 400)
                    # pd.set_option('display.max_columns', 50)
                    # df=pd.DataFrame(data_list)
                    # df=df.astype({'ListPrice':float})
                    # print(type(df['ModificationTimestamp']))
                    # print(df['ListPrice'].astype(float).describe().apply(lambda x: format(x, 'f')))
                    # if pid in  df['ListingID'].unique():
                    #     print('Listing id:{} is in the results:'.format(pid))
                    #     for index,row in df.iterrows():
                    #         # print("rrrrr {}".format(row["ListingID"]))
                    #         if(row['ListingID']==pid):
                    #             print(row)

                    # if env_vars.wn_listing_status != row['ListingStatus']:
                    #     logger.critical("The listing status is different in the direct mls and WN DB! {} versus {}".format(env_vars.wn_listing_status, row['ListingStatus']))
                    # else: logger.info("The direct mls and the wn db are matched")
                    # break
                    # else:
                    #     print('listing is not in the results')
                    # printTable(data_list)
                else:
                    message='no results were found'








        off_market_list=[]
        on_market_list=[]
        valid_active={}
        for dl in data_list:
            listing_status=dl.get('L_Status')
            listing_price=dl.get('L_AskingPrice')
            listing_id=dl.get('L_ListingID')
            listing_city=dl.get('L_City')
            addreess1=dl.get('L_AddressNumber')
            addreess2=dl.get('L_AddressSearchNumber')

            if listing_status == 'SOLD':
                off_market_list.append(listing_id)
            else:
                on_market_list.append(listing_id)
                if listing_price and int(listing_price) >= 30000 and (addreess1 != '' or addreess2 != ''):
                    valid_active[listing_id]=listing_city

        printTable(data_list)

    except:
        info=sys.exc_info()
        print(info)
