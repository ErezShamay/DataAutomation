import datetime
import json
import os
import re
import shutil
import sys
import time
import dateutil.parser
import psycopg2
import pytest
import pytz
import requests
import pandas as pd
import matplotlib.pyplot as plt
import csv
import matplotlib
import xmltodict

from subprocess import check_output, STDOUT
from pymongo import MongoClient
from reali_data.tests.monitoring.config import env_vars
from reali_data.tests.monitoring.config import params, gen_queries
from reali_data.tests.monitoring.config.gen_queries import provider_name_to_id
from src.common.gen_config import gen_params
from src.common.tools.aws.s3_aws import s3_class
from src.common.tools.aws.sqs import sqs_class
from src.common.tools.db_tools.rets import rets_class
from src.common.tools.gen_features import check_results_class
from src.common.tools.db_tools.postgress_db import postgress_class
from src.common.tools.db_tools.mongo_db import mongo_class
from src.common.tools.gen_features.enrichment_class import enrichment
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from requests.exceptions import *

rets_class = rets_class.rets_class()
s3_class = s3_class.s3_class()
sqs_class = sqs_class.sqs_class()
check_results_class = check_results_class.check_results_class()
postgress_class = postgress_class.postgress_class()
mongo_class = mongo_class.mongo_class()
enrichment_class = enrichment()

test = None
logger = ''
DAYS_PERIOD = 365

vendors2_test = params.vendors2_test
mls_vendor_type = gen_params.mls_vendor_type
vendors_map = gen_params.vendors_map
vendors_filters = gen_params.vendors_filters
vendors_join_filters = gen_params.vendors_join_filters
vendors_join_filters_short = gen_params.vendors_join_filters_short

DB_GOLD = gen_params.DB_GOLD
DB_MONGO = gen_params.DB_MONGO
DB_WN = gen_params.DB_WN
TRASHOLD = 500
START_DATE_SAMPLING = '1970-01-01'
END_DATE_SAMPLING = '2099-01-01'
SAMPLE_SINCE = 1577836800

res_dir_path = '{}/tmp/pods_memory/'.format(os.path.expanduser('~'))
THRESHOLD = 1

KUBE_ENVS = {
    'dev': 'dev.realatis.com',
    'prod': 'production.realatis.com',
    'qa': 'qa.realatis.com',
    'staging': 'staging.realatis.com'
}


def startLogger(logPath):
    import logging
    with open(logPath, 'w'):
        'just delete log'
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler(logPath)
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # add the env_vars to the logger
    log.addHandler(handler)
    return log


def psg_connect(connect=None):
    try:
        if connect is None:
            connect = psycopg2.connect(host=DB_WN['host'], port=DB_WN['port'], user=DB_WN['user'],
                                       password=DB_WN['password'], dbname=DB_WN['dbname'])
        cur = connect.cursor()
    except(ConnectionError, ConnectionRefusedError):
        connect = psycopg2.connect(host=DB_WN['host'], port=DB_WN['port'], user=DB_WN['user'],
                                   password=DB_WN['password'], dbname=DB_WN['dbname'])
        cur = connect.cursor()
    return connect, cur


def mongo_connect(envin, connect=None):
    if envin in ['dev', 'qa']:
        gen_params.mongo_coll = 'listings'
    if connect is None:
        connection_str = "mongodb://{}:{}@{}/{}?{}".format(DB_MONGO['user'], DB_MONGO['password'],
                                                           DB_MONGO['comps']['cluster'][envin],
                                                           DB_MONGO['comps']['dbname'][envin],
                                                           DB_MONGO['comps']['auth'][envin])
        client = MongoClient(connection_str)
        db = client[DB_MONGO['comps']['dbname'][envin]]
        connect = db[gen_params.mongo_coll]
    return connect


def set_query_psg(vendor='wn_crmls', test_type='properties'):
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    query_test_types = {
        'properties':
            "select display_mlsnumber from {}.{}_properties where {}".format(DB_WN['collection'], vendors_map[vendor],
                                                                             vendors_filters[vendor]),
        'properties_count':
            "select count(display_mlsnumber) "
            "from {}.{}_properties "
            "where {}".format(DB_WN['collection'], vendors_map[vendor], vendors_filters[vendor]),

        'pa_count':
            "select count({0}_properties.display_mlsnumber) "
            "from {1}.{0}_properties "
            "inner join {1}.{0}_property_attributes "
            "on {0}_properties.display_mlsnumber = {0}_property_attributes.property_id "
            "and {2}".format(vendors_map[vendor], DB_WN['collection'], vendors_join_filters[vendor]),

        'photos': "select * from {}.{}_photos ({where property_id in})".format(DB_WN['collection'], vendors_map[vendor],
                                                                               env_vars.photos_listing_ids.get(vendor)),

        'open_house1': "select mlnumber, fromdate, todate, fromtime, totime, listprice, status from {}.{}_open_house where "
                       "fromdate>'{}'".format(
            DB_WN['collection'], vendors_map[vendor], yesterday),
        'open_house': "select mlnumber,fromdate, todate, fromtime, totime, listprice, status "
                      "from {0}.{1}_open_house inner join {0}.{1}_properties on {0}.{1}_open_house.mlnumber = {0}.{1}_properties.display_mlsnumber"
                      " where {0}.{1}_open_house.fromdate>'{2}' "
                      "and {3}".format(
            DB_WN['collection'], vendors_map[vendor], yesterday, vendors_join_filters[vendor]),
        'duplication': "SELECT property_id, mls, hash_string, is_primary "
                       "FROM public.duplicate "
                       "where not is_primary",
        'white_list':
            "SELECT input_city, real_city "
            "FROM public.cities_dict where is_enabled",
    }
    query = query_test_types[test_type]
    return query


def get_time_frame(time_start, time_end):
    if time_start == '00:00' or time_end == '00:00' or not time_start or not time_end:
        time_frame = None
    else:
        time_frame = False
        t1 = time_start.split(':')
        h1 = t1[0]
        t2 = time_end.split(':')
        h2 = t2[0]
        if int(h1) < 10:
            h1 = h1.replace('0', '')  # remove leading zero from hour
            time_start = h1 + ':' + t1[1]
        if int(h2) < 10:
            h2 = h2.replace('0', '')  # remove leading zero from hour
            time_end = h2 + ':' + t2[1]
        if re.search('PM', time_start, flags=re.IGNORECASE) and re.search('PM', time_end, flags=re.IGNORECASE):
            time_frame = re.sub('PM', '', time_start, flags=re.IGNORECASE) + '-' + time_end
        elif re.search('AM', time_start, flags=re.IGNORECASE) and re.search('AM', time_end, flags=re.IGNORECASE):
            time_frame = re.sub('AM', '', time_start, flags=re.IGNORECASE) + '-' + time_end
        else:
            time_frame = time_start + '-' + time_end

        if not re.search('PM', time_frame, flags=re.IGNORECASE) and not re.search('AM', time_frame,
                                                                                  flags=re.IGNORECASE):
            t1 = time_start.split(':')
            h1 = t1[0]
            t2 = time_end.split(':')
            h2 = t2[0]
            if int(h1) > 12:
                start_time = str(int(h1) - 12) + ':' + t1[1] + 'PM'
            elif int(h1) < 12:
                if int(h1) == 0:
                    start_time = '12' + ':' + t1[1] + 'AM'
                else:
                    start_time = str(int(h1)) + ':' + t1[1] + 'AM'
            else:
                start_time = str(int(h1)) + ':' + t1[1] + 'PM'

            if int(h2) > 12:
                end_time = str(int(h2) - 12) + ':' + t2[1] + 'PM'
            elif int(h2) < 12:
                if int(h2) == 0:
                    end_time = '12' + ':' + t2[1] + 'AM'
                else:
                    end_time = str(int(h2)) + ':' + t2[1] + 'AM'
            else:
                end_time = str(int(h2)) + ':' + t2[1] + 'PM'

            if 'PM' in start_time and 'PM' in end_time:
                time_frame = start_time.replace('PM', '') + '-' + end_time
            elif 'AM' in start_time and 'AM' in end_time:
                time_frame = start_time.replace('AM', '') + '-' + end_time
            else:
                time_frame = start_time + '-' + end_time
    return time_frame


def get_expected_date_time(date_time):
    date_time = date_time.split('.')[0]
    date_time = dateutil.parser.parse(date_time)
    date_time = date_time.strftime('%Y-%m-%d %H:%M')
    get_date = date_time.split(' ')[0]
    get_time = date_time.split(' ')[1]
    return get_date, get_time


def get_date_time(expected_start, expected_end, expected_start_time=None, expected_end_time=None):
    expected_time_frame = None

    if expected_start_time and expected_start_time:
        expected_time_frame = get_time_frame(expected_start_time, expected_end_time)

    expected_start_date, expected_start_time = get_expected_date_time(expected_start)
    expected_end_date, expected_end_time = get_expected_date_time(expected_end)

    expected_end_date = expected_end.split('.')[0]
    expected_end_date = dateutil.parser.parse(expected_end_date)
    expected_end_date = expected_end_date.strftime('%Y-%m-%d-%h-%m-%s')

    if not expected_time_frame:
        expected_time_frame = get_time_frame(expected_start_time, expected_end_time)

    return expected_start_date, expected_end_date, expected_time_frame


def check_res_open_house(logger, vendor, postg_res, mong_res):
    res = False
    try:
        duplicted = []
        psg_duplicate_info = []
        mongo_dist_res = {}
        psg_dist_res = {}
        mis_lis = []
        plus_lis = []
        pres_ids = []
        mongo_mis_info = 0
        postg_dist_len = 0
        wrong_record = []
        psg_fault = []

        good_rec = []
        for mres in mong_res:  # set mongo_dist_res = a mongo id:openhouses_list dictionary
            if mres.get('vendorMlsId'):
                if mres.get('openHouse'):
                    if mres['vendorMlsId'] in mongo_dist_res:
                        duplicted.append(mres['vendorMlsId'])
                    else:
                        mongo_dist_res[mres['vendorMlsId']] = mres['openHouse']
                else:
                    mis_lis.append(mres.get('vendorMlsId'))
            else:
                mongo_mis_info += 1
                if not env_vars.mongo_no_mlsVendorId.get(vendor):
                    env_vars.mongo_no_mlsVendorId[vendor] = mres['_id']
                else:
                    env_vars.mongo_no_mlsVendorId[vendor].append(mres['_id'])

        for pres in postg_res:  # set psg_dist_res = a postgress id:openhouses_list dictionary
            if psg_dist_res.get(pres[0]):
                if pres not in psg_dist_res[pres[0]]:
                    psg_dist_res[pres[0]].append(pres)
                else:
                    psg_duplicate_info.append(pres[0])
            else:
                psg_dist_res[pres[0]] = [pres]

        for id, val in mongo_dist_res.items():  # check surplus
            if not psg_dist_res.get(id):
                plus_lis.append(id)

        for id, val in psg_dist_res.items():  # check missing and verify exists

            if id in env_vars.dup_ids[vendor]:
                # eliminate duplicated listings: listings marked as duplicated will not be considered as missing
                continue
            res_acc = []
            postg_dist_len += len(val)
            if not mongo_dist_res.get(id):
                mis_lis.append(id)
            else:
                if len(val) == len(mongo_dist_res.get(id)):
                    for i in range(len(mongo_dist_res.get(id))):
                        if id == '515496':
                            print(1)
                        act_date = mongo_dist_res.get(id)[i]['timestamp']
                        act_time_frame = mongo_dist_res.get(id)[i]['time']
                        act_date = datetime.datetime.fromtimestamp(act_date / 1000, tz=pytz.timezone("Etc/GMT+8"))
                        act_date = act_date.strftime('%Y-%m-%d')
                        for j in range(len(val)):
                            expected_start = val[j][1]
                            expected_end = val[j][2]
                            expected_start_time = val[j][3]
                            expected_end_time = val[j][4]

                            psg_start_date, psg_end_date, psg_time_frame = get_date_time(expected_start, expected_end,
                                                                                         expected_start_time,
                                                                                         expected_end_time)

                            if not psg_start_date or not psg_end_date or not psg_time_frame:
                                psg_fault.append(id)
                                break

                            if act_date == psg_start_date:
                                if act_time_frame.lower() == psg_time_frame.lower():
                                    good_rec.append(id)
                                    res_acc = []
                                    break
                                else:
                                    res_acc.append(False)

                            else:
                                res_acc.append(False)

                    if len(res_acc) == len(val):
                        wrong_record.append(id)

                else:
                    if len(val) > len(mongo_dist_res.get(id)):
                        mis_lis.append(id)
                    else:
                        plus_lis.append(id)

        if postg_dist_len == 0:
            logger.critical(
                '***** {} - Zero Active OpenHouse at WN -  wn = {}, comps = {} *****'.format(vendor, postg_dist_len,
                                                                                             len(mong_res)))

        if postg_dist_len == len(mongo_dist_res) and postg_dist_len != 0:
            res = True
            logger.info(
                "{} - wn_listings with open house = {}, comps listings with open house = {}, wrong info records = {}, missing records = {}, surplus records = {}".format(
                    vendor,
                    len(psg_dist_res),
                    len(mongo_dist_res),
                    len(wrong_record),
                    len(mis_lis),
                    len(plus_lis)))
            print(
                "{} - INFO -  wn_listings_with_open_house = {}, comps_listings_with_open_house = {}, wrong_info_records = {}, missing records = {}, surplus records = {}".format(
                    vendor,
                    len(psg_dist_res),
                    len(mongo_dist_res),
                    len(wrong_record),
                    len(mis_lis),
                    len(plus_lis)))
        else:
            res = False
            logger.critical(
                '{} - wn listings with open house = {}, comps listings with open house = {}, wrong info records = {}, missing_records = {}, surplus_records = {}'.format(
                    vendor,
                    len(psg_dist_res),
                    len(mongo_dist_res),
                    len(wrong_record),
                    len(mis_lis),
                    len(plus_lis)))
            print(
                '{} - CRITICAL - wn listings with open house = {}, comps listings with open house = {}, wrong info records = {}, missing records = {}, surplus records = {}'.format(
                    vendor,
                    len(psg_dist_res),
                    len(mongo_dist_res),
                    len(wrong_record),
                    len(mis_lis),
                    len(plus_lis)))

        print(
            '\n****{} - OPEN_HOUSE Details\nwrong info records = {},\nmissing records = {},\nsurplus records = {}\nwn_fault_rec={}****\n'.format(
                vendor,
                wrong_record,
                mis_lis,
                plus_lis, psg_fault))

        if mongo_mis_info > 0:
            res = False
            logger.critical('{} - comps listing with no vendorMlsId = {}'.format(vendor, mongo_mis_info))
            print('{} - comps listing with no vendorMlsId = {}\n{}'.format(vendor, mongo_mis_info,
                                                                           env_vars.mongo_no_mlsVendorId[vendor]))

    except():
        res = False
        temp = sys.exc_info()
    return res


def check_res_photos(logger, vendor, postg_res, mongo_photos):
    try:
        mis_lis = []
        plus_lis = []
        mongo_surp_info = 0
        mongo_mis_info = 0
        psg_photos = {}
        total_wn = 0
        total_comps = 0
        default_two = 0
        default_two_lis = []
        low_number_psg = []
        mongo_plus_key_lis = []
        for id in postg_res:
            if not psg_photos.get(id[0]):
                psg_photos[id[0]] = 1
            else:
                psg_photos[id[0]] += 1

        for key, val in mongo_photos.items():
            if key in psg_photos:
                if mongo_photos[key] > 0 and mongo_photos[key] == psg_photos[key]:
                    continue
                if psg_photos[key] == 0:
                    logger.critical(
                        '***** {} - property_id = {} - Zero photos at WN -  wn = {}, comps = {} *****'.format(vendor,
                                                                                                              key,
                                                                                                              psg_photos[
                                                                                                                  key],
                                                                                                              mongo_photos[
                                                                                                                  key]))
                    low_number_psg.append(key)
                elif psg_photos[key] < 3:
                    low_number_psg.append(key)
                if mongo_photos[key] == 0:
                    logger.critical(
                        '***** {} - property_id = {} - Zero photos at comps -  wn = {}, comps = {} *****'.format(vendor,
                                                                                                                 key,
                                                                                                                 mongo_photos[
                                                                                                                     key],
                                                                                                                 mongo_photos[
                                                                                                                     key]))

                elif mongo_photos[key] < 3 and psg_photos[key] > 2:
                    default_two += 1
                    default_two_lis.append(key)

                elif mongo_photos[key] != psg_photos[key]:
                    if mongo_photos[key] > psg_photos[key]:
                        mongo_surp_info += 1
                        plus_lis.append(key)
                    else:
                        mongo_mis_info += 1
                        mis_lis.append(key)
                total_wn += psg_photos[key]
                total_comps += mongo_photos[key]
            else:
                mongo_plus_key_lis.append(key)
        report = (
            '{} - missing = {}, surplus_photos = {}, wn_less_than_3_photos = {}, comps_default_2_photos = {}, comps_surplus_ids_not_in_wn_photos = {}'.format(
                vendor, len(mis_lis),
                len(plus_lis), len(low_number_psg), default_two,
                len(mongo_plus_key_lis)))
        if total_wn == total_comps and len(mis_lis) == 0:
            res = True
            logger.info(report)
            print(report)
        else:
            res = False
            logger.critical(report)
            print(report)

        report_detailed = (
            '\n****{} - PHOTOS Details\nmissing = {},\nsurplus_photos = {},\nwn_less_than_3_photos = {},\ncomps_default_2_photos = {},\ncomps_surplus_listings = {}****\n'.format(
                vendor, mis_lis, plus_lis, low_number_psg, default_two_lis, mongo_plus_key_lis))
        print(report_detailed)

        if env_vars.photos_failure_info.get(vendor):
            res = False
            logger.critical('{} - comps listing with photos but no vendorMlsId = {}'.format(vendor, len(
                env_vars.photos_failure_info[vendor])))
            for err in env_vars.photos_failure_info[vendor]:
                print(err)
    except():
        res = False
        temp = sys.exc_info()
        print(temp)
    return res


def date_to_epoch(date_in):
    epoch_time = None
    if date_in:
        tz = pytz.timezone('UTC')
        if ' ' in date_in:
            date_format = '%Y-%m-%d %H:%M:%S'
        else:
            date_format = '%Y-%m-%d'
        dt1 = datetime.datetime.strptime(date_in, date_format)
        epoch_time = datetime.datetime(dt1.year, dt1.month, dt1.day, tzinfo=tz).timestamp()
        if isinstance(epoch_time, float):
            epoch_time = int(epoch_time)
    return epoch_time * 1000


def get_act_listings_back_last(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                               end_date_in=None):
    vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)

    try:
        dup_res_dict = {}
        test_type_queries = test_type
        mls_query = "select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                    "where not is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))
        postg_res = postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        for res in postg_res:
            property_id = res[0].get('property_id')
            dup_res_dict[property_id] = 1
        env_vars.dup_res_dict[vendor] = dup_res_dict
        mongo_res_photos_count = {}
        mongo_res_photos_count[vendor] = {}
        query_mongo = {'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                       'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                       'location.address1': {'$ne': 'Undisclosed Address'},
                       'currentListing.listingType': {'$ne': 'REALI'}
                       }
        if start_date_in and end_date_in:
            test_type_queries = 'properties_start_end_date'
        elif start_date_in:
            test_type_queries = 'properties_start_date'
        elif end_date_in:
            test_type_queries = 'properties_end_date'
        projection = {"item1": 1,
                      "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                      "item3": 1,
                      "vendorMlsId": "$currentListing.vendorMlsId",
                      "item4": 1,
                      "realiUpdated": "$realiUpdated",
                      "item5": 1,
                      "listingStatus": "$currentListing.listingStatus",
                      "listDate": "$currentListing.listDate"
                      }
        full_query = [
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args = get_db_mongo_args(envin, 'listings')
        mongo_res_dict = {}
        connect = None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client = MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net",
                                 ssl=True,
                                 port=27017)
            db = client['realidb-production']
            connect = db['listings']
        mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        for mres in mongo_res:
            listing_id = mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id] = 1
                mongo_res_photos_count[vendor][listing_id] = mres.get('assets')
            else:
                mongo_res_dict[listing_id] += 1
        mls_query = gen_queries.get_query(test_type_queries, vendor, start_date_in=start_date_in,
                                          end_date_in=end_date_in)
        postg_res = postgress_class.get_query_psg(DB_WN, mls_query)
        postg_res_dict = {}
        for pres in postg_res:
            listing_id = pres[0].get('property_id')
            postg_res_dict[listing_id] = pres[0].get('city')
        mongo_res_photos_count[vendor] = keep_valid_mongo(postg_res_dict, mongo_res_photos_count[vendor])
    except():
        info = sys.exc_info()
        print(info)
        postg_res_dict = {}
        mongo_res_dict = {}
        mongo_res_photos_count = 0
    return vendor, postg_res_dict, mongo_res_dict, mongo_res_photos_count


def get_act_listings_back(logger, envin, params, vendor, test_type='properties', start_date_in=None, end_date_in=None):
    vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
    try:
        dup_res_dict = {}
        test_type_queries = test_type
        mls_query = "select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                    "where not is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))
        postg_res = postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        for res in postg_res:
            property_id = res[0].get('property_id')
            dup_res_dict[property_id] = 1
        env_vars.dup_res_dict[vendor] = dup_res_dict
        mongo_res_photos_count = {}
        mongo_res_photos_count[vendor] = {}
        query_mongo = {'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                       'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                       'location.address1': {'$ne': 'Undisclosed Address'},
                       'currentListing.listingType': {'$ne': 'REALI'}
                       }

        if start_date_in and end_date_in:
            test_type_queries = 'properties_start_end_date'
        elif start_date_in:
            test_type_queries = 'properties_start_date'
        elif end_date_in:
            test_type_queries = 'properties_end_date'
        projection = {"item1": 1,
                      "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                      "item3": 1,
                      "vendorMlsId": "$currentListing.vendorMlsId",
                      "item4": 1,
                      "realiUpdated": "$realiUpdated",
                      "item5": 1,
                      "listingStatus": "$currentListing.listingStatus",
                      "listDate": "$currentListing.listDate"
                      }
        full_query = [
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args = get_db_mongo_args(envin, 'listings')
        mongo_res_dict = {}
        connect = None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client = MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net",
                                 ssl=True,
                                 port=27017)
            db = client['realidb-production']
            connect = db['listings']
        mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        for mres in mongo_res:
            listing_id = mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id] = 1
                mongo_res_photos_count[vendor][listing_id] = mres.get('assets')
            else:
                mongo_res_dict[listing_id] += 1
        mls_query = gen_queries.get_query(test_type_queries, vendor, start_date_in=start_date_in,
                                          end_date_in=end_date_in)
        postg_res = postgress_class.get_query_psg(DB_WN, mls_query)
        postg_res_dict = {}
        for pres in postg_res:
            listing_id = pres[0].get('property_id')
            postg_res_dict[listing_id] = pres[0].get('city')
        mongo_res_photos_count[vendor] = keep_valid_mongo(postg_res_dict, mongo_res_photos_count[vendor])
    except():
        info = sys.exc_info()
        print(info)
        postg_res_dict = {}
        mongo_res_dict = {}
        mongo_res_photos_count = 0
    return vendor, postg_res_dict, mongo_res_dict, mongo_res_photos_count


def get_act_listings_is_app(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                            end_date_in=None):
    vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
    try:
        time_back = datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
        time_back_epoch = int(time_back.timestamp())
        dup_res_dict = {}
        test_type_queries = test_type

        mls_query = "select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                    "where is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))

        postg_res = postgress_class.get_query_psg(DB_GOLD[envin], mls_query)

        query_mongo = {'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                       'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                       'location.address1': {'$ne': 'Undisclosed Address'},
                       'currentListing.listDate': {'$gte': time_back_epoch}
                       }
        projection = {"item1": 1,
                      "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                      "item3": 1,
                      "vendorMlsId": "$currentListing.vendorMlsId",
                      "item4": 1,
                      "realiUpdated": "$realiUpdated",
                      "item5": 1,
                      "listingStatus": "$currentListing.listingStatus",
                      "listDate": "$currentListing.listDate"
                      }
        full_query = [
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args = get_db_mongo_args(envin, 'listings')
        mongo_res_dict = {}
        connect = None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client = MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net",
                                 ssl=True,
                                 port=27017)
            db = client['realidb-production']
            connect = db['listings']
        mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        for mres in mongo_res:
            listing_id = mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id] = 1
            else:
                mongo_res_dict[listing_id] += 1
        mongo_res_dict_on = mongo_res_dict
        mongo_res_dict = {}

        query_mongo = {'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                       'currentListing.listingStatus': {'$exists': True, '$in': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                       'location.address1': {'$ne': 'Undisclosed Address'},
                       'currentListing.listDate': {'$gte': time_back_epoch}
                       }
        projection = {"item1": 1,
                      "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                      "item3": 1,
                      "vendorMlsId": "$currentListing.vendorMlsId",
                      "item4": 1,
                      "realiUpdated": "$realiUpdated",
                      "item5": 1,
                      "listingStatus": "$currentListing.listingStatus",
                      "listDate": "$currentListing.listDate"
                      }
        full_query = [
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args = get_db_mongo_args(envin, 'listings')
        mongo_res_dict = {}
        connect = None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client = MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net",
                                 ssl=True,
                                 port=27017)
            db = client['realidb-production']
            connect = db['listings']
        mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        for mres in mongo_res:
            listing_id = mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id] = 1
            else:
                mongo_res_dict[listing_id] += 1

        mongo_res_dict_off = mongo_res_dict
        mongo_res_dict = {}
        postg_res_dict = {}
        for pres in postg_res:
            listing_id = pres[0].get('property_id')
            postg_res_dict[listing_id] = 1
    except():
        info = sys.exc_info()
        print(info)
        postg_res_dict = mongo_res_dict_on = mongo_res_dict_off = {}
    return vendor, postg_res_dict, mongo_res_dict_on, {}, mongo_res_dict_off


def get_act_listings_is_not_primary(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                                    end_date_in=None):
    mongo_res_dict_off = {}
    vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
    try:
        time_back = datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
        time_back_epoch = int(time_back.timestamp())
        dup_res_dict = {}
        test_type_queries = test_type
        mls_query = "select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                    "where (not is_app_primary or is_app_primary is null)  and mls = '{}') t".format(
            gen_params.mls_vendor_type.get(vendor))
        postg_res = postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        query_mongo = {'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                       'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                       'location.address1': {'$ne': 'Undisclosed Address'},
                       'currentListing.listDate': {'$gte': time_back_epoch}
                       }
        projection = {"item1": 1,
                      "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                      "item3": 1,
                      "vendorMlsId": "$currentListing.vendorMlsId",
                      "item4": 1,
                      "realiUpdated": "$realiUpdated",
                      "item5": 1,
                      "listingStatus": "$currentListing.listingStatus",
                      "listDate": "$currentListing.listDate"
                      }
        full_query = [
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args = get_db_mongo_args(envin, 'listings')
        mongo_res_dict = {}

        connect = None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client = MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net",
                                 ssl=True,
                                 port=27017)
            db = client['realidb-production']
            connect = db['listings']
        mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        for mres in mongo_res:
            listing_id = mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id] = 1
            else:
                mongo_res_dict[listing_id] += 1
        mongo_res_dict_on = mongo_res_dict
        postg_res_dict = {}
        for pres in postg_res:
            listing_id = pres[0].get('property_id')
            postg_res_dict[listing_id] = 1
    except():
        info = sys.exc_info()
        print(info)
        postg_res_dict = mongo_res_dict_on = mongo_res_dict_off = {}
    return vendor, postg_res_dict, mongo_res_dict_on, {}, mongo_res_dict_off


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def get_db_mongo_args_back(envin, collection):
    db_mongo_args = {
        'dbname': DB_MONGO['listings']['dbname'][envin],
        'collection': collection,
        'cluster': DB_MONGO['listings']['cluster'][envin],
        'user': DB_MONGO['user'],
        'password': DB_MONGO['password'],
        'auth': DB_MONGO['listings']['auth'][envin]
    }
    return db_mongo_args


def get_db_mongo_args(envin, collection):
    # this function will get the relevant mongo arguments from the secrets connection string
    dbname = None
    port = None
    to_db_name = DB_MONGO.split('/')
    for tdn in to_db_name:
        if '?' in tdn:
            dbname = tdn.split('?')[0]
            break
    cluster_full = DB_MONGO.split(',')[0]
    cluster = re.split(':\d+', cluster_full)[0]

    has_port = re.findall(':\d+', cluster_full)
    if len(has_port) > 0:
        port = has_port[0].split(':')[1]
        if port.isnumeric():
            port = int(port)
    db_mongo_args = {
        'collection': collection,
        'cluster': cluster,
        'port': port,
        'dbname': dbname
    }
    return db_mongo_args


def keep_valid_mongo(postg_res, mongo_res):
    plus_lis = check_results_class.which_not_in(postg_res, mongo_res)
    for plis in plus_lis:
        mongo_res.pop(plis)
    return mongo_res


def summary(test_type, envin):
    if test_type and test_type in ['properties_24', 'properties_time_frame']:
        env_vars.plus_lis = []
    res = False
    env_vars.logger.info(
        '\nTest summary: Mongo Vs Fact - {} Environemnt.\nFor full report, review "test_active_wn_vs_mongo"'.format(
            envin))
    if env_vars.from_source_count:
        mis_alignment_ratio = 100 * (len(env_vars.mis_lis) + len(env_vars.plus_lis) + len(
            env_vars.no_assets)) / env_vars.from_source_count
    else:
        mis_alignment_ratio = None
    if mis_alignment_ratio and mis_alignment_ratio > 1 or len(env_vars.mis_lis) > TRASHOLD or len(
            env_vars.plus_lis) > TRASHOLD:
        env_vars.logger.critical(
            '\nTotal in Source = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicate ignored = {}\n'.format(
                env_vars.from_source_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
        print(
            'Test Fail:\nTotal in Source = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicate ignored = {}'.format(
                env_vars.from_source_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
    else:
        res = True
        env_vars.logger.info(
            '\nTotal in Source = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicate ignored = {}'.format(
                env_vars.from_source_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
        print(
            'Test Pass - Total in Source = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicate ignored = {}'.format(
                env_vars.from_source_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
    return res


def set_postgress_db_from_secret_jdbc(service_name, db_source, aws_secrets):
    db_secrets = {}
    host = None
    port = None
    dbname = None
    try:
        con_str = aws_secrets.get(service_name).get('db').get(db_source).get('url')
        con_str_elements = con_str.split('/')
        for ce in con_str_elements:
            if 'com:' in ce:
                ce_host_port_list = ce.split(':')
                host = ce_host_port_list[0]
                port = ce_host_port_list[1]
            elif '?' in ce:
                dbname_list = ce.split('?')
                dbname = dbname_list[0]
        if host and port and dbname and port.isnumeric:
            port = int(port)
            db_secrets = {
                'host': host,
                'port': port,
                'dbname': dbname,
                'user': aws_secrets.get(service_name).get('db').get(db_source).get('user'),
                'password': aws_secrets.get(service_name).get('db').get(db_source).get('password'),
            }
    except():
        print('Failed to retrieve secrets for {} - Please verify Secret config exists'.format(service_name))
    return db_secrets


def get_set_secrerts_back(envin):
    db_mongo = {}
    db_gold = {}
    db_wn = {}
    try:
        services = ['de-extractor', 'de-saver', 'comp-proxy']
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf = s3_class.get_secrets_by_service(envin, service_name, params)
                env_vars.aws_secrets[service_name] = conf

        db_gold[envin] = set_postgress_db_from_secret_jdbc('de-saver', db_source='rds')
        db_wn = set_postgress_db_from_secret_jdbc('de-extractor', db_source='wolfnet')
        db_mongo = env_vars.aws_secrets['comp-proxy'].get('mongodb').get('uri')
    except():
        info = sys.exc_info()
        print(info)
    return db_mongo, db_gold, db_wn


def get_set_secrerts(envin, services):
    aws_secrets = {}
    try:
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf = s3_class.get_secrets_by_service(envin, service_name, params)
                aws_secrets[service_name] = conf
    except():
        info = sys.exc_info()
        print(info)
    return aws_secrets


def res_2_html(pod_name):
    ser = []
    with open(res_dir_path + env_vars.res_file_name, 'r') as ms:
        reader = csv.reader(ms)
        for row in reader:
            ser.append(int(', '.join(row)))
    s = pd.DataFrame({pod_name: ser, 'Threshold': [THRESHOLD] * len(ser)})

    fig, ax = plt.subplots()
    fig.patch.set_facecolor('xkcd:dark grey')
    ax.set_facecolor('xkcd:almost black')
    ax.set_title(pod_name + ' Memory Status', color='white')
    ax.set_xlabel('Build Samples', color='white')
    ax.set_ylabel('Memory(Mbit)', color='white')
    mng = plt.get_current_fig_manager()
    mng.full_screen_toggle()
    plt.plot(ser, color='yellow', label='Memory Usage')
    plt.plot([THRESHOLD] * len(ser), color='red', label='Memory Threshold')
    plt.legend(loc='best')
    fig.savefig('{}'.format(env_vars.plot_file_name), transparent=True)
    plt.close(fig)
    with open('{}_mem_status.html'.format(pod_name), 'w') as report:
        report.write("""<html>
        <head><title></timem_size_oftle></head>
        <body>
        <style> body {{background-color: 404040}} </style>
        <img src='{}'>
        </body>
        </html>""".format(env_vars.plot_file_name))


def get_pods_mem_size(envin, pod_name):
    mem_size_of = None
    shell_command = 'kubectl config use-context {}'.format(KUBE_ENVS[envin])
    uc = check_output(shell_command, shell=True, stderr=STDOUT,
                      universal_newlines=True).rstrip('\n')
    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(uc))
    shell_command = 'kubectl get pods | grep {} |cut -f 1 -d " "'.format(pod_name)
    last_pod = check_output(shell_command, shell=True, stderr=STDOUT,
                            universal_newlines=True).rstrip('\n')
    if 'timeout' in last_pod:
        env_vars.logger.critical(
            'Could not get {}, pd info, got {}'.format(pod_name, last_pod))
        return mem_size_of
    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(last_pod))
    shell_command = "kubectl top pod {} | awk '{{print $3}}'".format(last_pod)
    mem_status = check_output(shell_command, shell=True, stderr=STDOUT,
                              universal_newlines=True).rstrip('\n')
    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(mem_status))
    mem_size = re.finditer(r"\d+", mem_status, re.MULTILINE)
    for matchNum, match in enumerate(mem_size, start=1):
        print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum=matchNum, start=match.start(),
                                                                            end=match.end(), match=match.group()))
        mem_size_of = match.group()
    print('Getting mem_size of: {}'.format(mem_size_of))
    return mem_size_of


def check_res(mem_size_of):
    if int(mem_size_of) > THRESHOLD:
        res = False
        print('fail - Memory size is {}'.format(mem_size_of))
        env_vars.logger.critical('Memory size is {}'.format(mem_size_of))

    else:
        res = True
        print('pass - memory size is {}'.format(mem_size_of))
        env_vars.logger.info('Memory size is {}'.format(mem_size_of))

    return res


def record_memory_size(rec_mem_size_file, mem_size_of):
    if os.path.exists(rec_mem_size_file):
        with open(rec_mem_size_file, 'a') as ms:
            writer = csv.writer(ms)
            writer.writerow([mem_size_of])
    else:
        with open(rec_mem_size_file, 'w') as ms:
            writer = csv.writer(ms)
            writer.writerow([mem_size_of])
    print('saving results record to {}'.format(rec_mem_size_file))
    with open(rec_mem_size_file, 'r') as ms:
        reader = csv.reader(ms)
        for row in reader:
            print(', '.join(row))


def copy_records_for_jenkins_artifacts(rec_mem_size_file):
    shutil.copyfile(rec_mem_size_file, env_vars.res_file_name)
    shutil.copyfile(env_vars.plot_file_name, res_dir_path + env_vars.plot_file_name)
    shutil.copyfile(env_vars.plot_html_name, res_dir_path + env_vars.plot_html_name)
    print('plot file is at {}'.format(res_dir_path + env_vars.plot_html_name))


def get_act_fact_listings(db_args, listings_list):
    listings = str(listings_list).replace('[', '').replace(']', '')
    postg_res_dict = {}
    mls_query = "select row_to_json (t) from (SELECT * FROM public.fact_listings where mlsid in({})) t".format(listings)
    postg_res = postgress_class.get_query_psg(db_args, mls_query)
    for res in postg_res:
        mls_id = res[0].get('mlsid')
        postg_res_dict[mls_id] = {
            'city': res[0].get('city'),
            'real_city': res[0].get('real_city'),
            'status': res[0].get('status'),
            'is_duplicated': res[0].get('is_duplicated'),
            'primary_listing_id': res[0].get('primary_listing_id'),
            'is_filter': res[0].get('is_filter'),
            'filter_id': res[0].get('filter_id'),
            'originatingsystemname': res[0].get('originatingsystemname'),
            'propertytype': res[0].get('propertytype'),
            'subtype': res[0].get('subtype')
        }
    return postg_res_dict


def get_resp_properties_and_agent_key_numeric_for_rest_api(res_json, agg_res):
    photos_count = 0
    for rej in res_json.get('value'):
        listing_status = rej.get('StandardStatus')
        if listing_status not in ['Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete']:
            mls_id = rej.get('ListingId')
            if 'C' not in mls_id and 'R' not in mls_id:
                listing_price = rej.get('ListPrice')
                listing_state = rej.get('StateOrProvince')
                listing_lot_size = rej.get('LotSizeSquareFeet')
                listing_sqft = rej.get('LivingArea')
                zip_code = rej.get('PostalCode')
                house_type = rej.get('HouseType')
                address = rej.get('UnparsedAddress')
                if rej.get('ListingAgentKeyNumeric') is not None:
                    listing_agent_key_numeric = rej.get('ListingAgentKeyNumeric')
                else:
                    listing_agent_key_numeric = rej.get('ListAgentKeyNumeric')
                if listing_price is not None and listing_state == 'CA' and listing_lot_size is not None \
                        and listing_sqft is not None and zip_code is not None and address is not None \
                        and house_type != 'UNSUPPORTED':
                    if rej.get('Media'):
                        photos_count = len(rej.get('Media'))  # refer to mls_listings and bayeast
                    elif rej.get('PhotosCount'):
                        photos_count = int(rej.get('PhotosCount'))  # refer to all of the other mls's
                    if agg_res.get(mls_id) is None:
                        if listing_agent_key_numeric is not None and listing_agent_key_numeric != '':
                            agg_res[mls_id] = [{'PhotosCount': photos_count,
                                                'ListingAgentKeyNumeric': str(listing_agent_key_numeric)}]
                        else:
                            agg_res[mls_id] = [{'PhotosCount': photos_count, 'ListingAgentKeyNumeric': None}]
    return agg_res


def get_resp_properties_and_agent_key_numeric(res_json, agg_res):
    keys_list = res_json.keys()
    values_list = res_json.values()
    photos_count = 0
    for key in keys_list:
        for value in values_list:
            listing_status = value[0].get('StandardStatus')
            if listing_status not in ['Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete']:
                mls_id = key
                listing_price = value[0].get('ListPrice')
                listing_state = value[0].get('StateOrProvince')
                listing_lot_size = value[0].get('LotSizeSquareFeet')
                listing_sqft = value[0].get('LivingArea')
                zip_code = value[0].get('PostalCode')
                house_type = value[0].get('HouseType')
                address = value[0].get('UnparsedAddress')
                if value[0].get('ListingAgentKeyNumeric') is not None:
                    listing_agent_key_numeric = value[0].get('ListingAgentKeyNumeric')
                else:
                    listing_agent_key_numeric = value[0].get('ListAgentKeyNumeric')
                if listing_price is not None and listing_state == 'CA' and listing_lot_size is not None \
                        and listing_sqft is not None and zip_code is not None and address is not None \
                        and house_type != 'UNSUPPORTED':
                    if value[0].get('Media'):
                        photos_count = len(value[0].get('Media'))  # refer to mls_listings and bayeast
                    elif value[0].get('PhotosCount'):
                        photos_count = int(value[0].get('PhotosCount'))  # refer to all of the other mls's
                    if agg_res.get(mls_id) is None:
                        if listing_agent_key_numeric is not None and listing_agent_key_numeric != '':
                            agg_res[mls_id] = [{'PhotosCount': photos_count,
                                                'ListingAgentKeyNumeric': str(listing_agent_key_numeric)}]
                        else:
                            agg_res[mls_id] = [{'PhotosCount': photos_count, 'ListingAgentKeyNumeric': None}]
    return agg_res


def build_agg_res_agents(res_json, agg_res_agents):
    if res_json.get('value'):
        for rej in res_json.get('value'):
            agent_full_name = rej.get('ListAgentFullName')
            agent_email = rej.get('ListAgentEmail')
            agent_phone = rej.get('ListAgentPreferredPhone')
            agent_dre = rej.get('ListAgentStateLicense')
            office_name = rej.get('ListOfficeName')
            agg_res_agents['agents_info'].append({'memberfullname': agent_full_name, 'memberemail': agent_email,
                                                  'memberpreferredphone': agent_phone, 'memberstatelicense': agent_dre,
                                                  'officename': office_name})
    return agg_res_agents


def get_mls_listing_api(req, headers):
    res = requests.get(req, headers=headers)
    res_json = json.loads(res.content)
    if res_json.get('Message') == 'Your token can access IDX feed only':
        while res_json.get('Message'):
            print(res_json.get('Message'))
            res = requests.get(req, headers=headers)
            res_json = json.loads(res.content)
    if res_json.get('Message') == 'The wait operation timed out':
        print(res_json.get('Message'))
        res_json = False
    return res_json


def get_rets_mls_listing_api(req, vendor, class_option=None, cookies=None, service_flag=None):
    count = 0
    dict_data = {}
    try:
        res = requests.get(req, cookies=cookies)
        res_json = xmltodict.parse(res.content)
        if service_flag == 'property':
            count, dict_data = parse_res_properties_with_params(res_json, class_option)
        elif service_flag == 'agents':
            count, dict_data = parse_res_agents_with_params(res_json)
        elif service_flag == 'open_house':
            dict_data = parse_res_open_house_events_with_params(vendor, res_json)
            return dict_data
    except(Timeout, TooManyRedirects, RequestException) as err:
        print('Error in getting response:\n', err)
        raise err
    return count, dict_data


def sum_sqft_values(li: list):
    living_area_square_feet = 0
    counter = 0
    for i in li:
        if i == '':
            i = 0
            living_area_square_feet += i
        else:
            living_area_square_feet += int(i)
        counter += 1
        if counter == 4:
            break
    living_area_square_feet = str(living_area_square_feet)
    return living_area_square_feet


def parse_res_properties_with_params(res_json, class_option=None):
    dict_data = {}
    count = 0
    try:
        if res_json.get('RETS').get('COUNT') is not None:
            count = res_json.get('RETS').get('COUNT').get('@Records')
            list_data = res_json.get('RETS').get('DATA')
            assert list_data is not None
            for item in list_data:
                li = list(item.split("\t"))
                if len(li) >= 16 and (class_option == 'INCP' or class_option == 'RI_2'):
                    living_area_square_feet = sum_sqft_values(li)
                    dict_data[li[8]] = [{'ListPrice': li[6], 'City': li[7], 'StandardStatus': li[11],
                                         'StateOrProvince': li[10], 'ModificationTimeStamp': li[13],
                                         'UnparsedAddress': li[5],
                                         'PhotosCount': li[9], 'HouseType': li[12], 'LotSizeSquareFeet': li[4],
                                         'LivingArea': living_area_square_feet, 'PostalCode': li[14],
                                         'ListingAgentKeyNumeric': li[15]}]
                elif len(li) >= 13:
                    dict_data[li[5]] = [{'ListPrice': li[3], 'City': li[4], 'StandardStatus': li[8],
                                         'StateOrProvince': li[7], 'ModificationTimeStamp': li[10],
                                         'PhotosCount': li[6], 'HouseType': li[9], 'UnparsedAddress': li[2],
                                         'LotSizeSquareFeet': li[1], 'LivingArea': li[0], 'PostalCode': li[11],
                                         'ListingAgentKeyNumeric': li[12]}]
            print('finished parsing the offset response')
    except AssertionError:
        print('Error in parsing res')
    return count, dict_data


def parse_res_agents_with_params(res_json):
    dict_data = {}
    count = 0
    try:
        list_of_valid_values = []
        count = res_json.get('RETS').get('COUNT').get('@Records')
        list_data = res_json.get('RETS').get('DATA')
        assert list_data is not None
        for item in list_data:
            li = list(item.split("\t"))
            if len(li) >= 5:
                list_of_valid_values.append(
                    {'ListAgentFullName': li[0], 'ListAgentEmail': li[1], 'ListAgentPreferredPhone': li[2],
                     'ListAgentStateLicense': li[3], 'ListOfficeName': li[4]})
        print('finished parsing the offset response')
        dict_data['value'] = list_of_valid_values
    except AssertionError:
        print('Error in parsing res')
    return count, dict_data


def parse_res_open_house_events_with_params(vendor, res_json):
    dict_data = {}
    index_list = []
    try:
        columns = res_json.get('RETS').get('COLUMNS')
        list_data = res_json.get('RETS').get('DATA')
        assert columns and list_data is not None
        columns_list = list(columns.split("\t"))
        params_list = get_open_house_events_params(vendor)
        for param in params_list:
            index_list.append(int(columns_list.index(param)))
        open_hose_key_index = index_list[0]
        listing_key_index = index_list[1]
        open_house_start_time_index = index_list[2]
        open_house_end_time_index = index_list[3]
        for item in list_data:
            li = list(item.split("\t"))
            dict_data = build_open_house_event_dict(li, open_hose_key_index, listing_key_index,
                                                    open_house_start_time_index, open_house_end_time_index, dict_data)
        print('finished parsing the offset response')
    except AssertionError:
        print('Error in parsing res')
    return dict_data


def get_open_house_events_params(vendor):
    params_dict = {'crmls': ['OpenHouseKeyNumeric', 'ListingId', 'OpenHouseDate', 'OpenHouseEndTime2'],
                   'sfar': ['OpenHouseRid', 'ListingRid', 'OpenHouseDate', 'EndTime'],
                   'sdmls': ['OH_UniqueID', 'L_ListingID', 'OH_StartDateTime', 'OH_EndDateTime'],
                   'metrolist': ['OpenHouseRid', 'ListingRid', 'StartTime', 'EndTime'],
                   'bareis': ['OpenHouseRid', 'ListingRid', 'OpenHouseDate', 'EndTime']}
    return params_dict.get(vendor)


def build_open_house_event_dict(li, open_hose_key_index, listing_key_index, open_house_start_time_index,
                                open_house_end_time, dict_data):
    try:
        if dict_data.get(li[listing_key_index]) is None:
            dict_data[li[listing_key_index]] = [{'openhousekey': li[open_hose_key_index],
                                                 'openhousestarttime': li[open_house_start_time_index],
                                                 'openhouseendtime': li[open_house_end_time]}]
        else:
            dict_data[li[listing_key_index]].append({'openhousekey': li[open_hose_key_index],
                                                     'openhousestarttime': li[open_house_start_time_index],
                                                     'openhouseendtime': li[open_house_end_time]})
    except IndexError:
        pass
    return dict_data


def get_db_mongo_config(envin, aws_secrets):
    if envin == 'prod':
        db_mongo = aws_secrets['qa-comp-proxy'].get('mongodb').get('uri')
    else:
        db_mongo = aws_secrets['comp-proxy'].get('mongodb').get('uri')
    return db_mongo


def base_request(provider: str, since_time: str, to_time: str, limit: int, offset: int, class_option=None,
                 service_flag=None):
    headers = {}
    request = ''
    if provider == 'mls_listings':
        if service_flag == 'property':
            headers['Authorization'] = 'Bearer 4e3f0ecdc65b1756a7a4bb10b55ffa4f'
            request = "https://data.api.mlslistings.com/full/Property?$count=true&" \
                      "$filter=" \
                      "(ModificationTimestamp ge {}) " \
                      "and (ModificationTimestamp le {})" \
                      "and (PropertyType has ResourceEnums.PropertyType'Residential' " \
                      "or PropertyType has ResourceEnums.PropertyType'ResidentialIncome')".format(since_time, to_time)
        if service_flag == 'open_house':
            headers['Authorization'] = 'Bearer 4e3f0ecdc65b1756a7a4bb10b55ffa4f'
            request = "https://data.api.mlslistings.com/full/OpenHouse?" \
                      "$filter=ModificationTimestamp ge {}" \
                      "&$count=true".format(since_time)
    elif provider == 'bayeast':
        if service_flag == 'property':
            headers['Authorization'] = 'Bearer 99a19235ed032619b2bb94bc33fb8fb0'
            request = "https://api.bridgedataoutput.com/api/v2/OData/beccar/Property?$filter= " \
                      "(PropertyType eq 'Residential' or PropertyType eq 'Residential_Income') " \
                      "and (BridgeModificationTimestamp ge {} and BridgeModificationTimestamp le {}) " \
                      "and (OriginatingSystemName eq 'BAY EAST' or OriginatingSystemName eq 'WALNUT CREEK' " \
                      "or OriginatingSystemName eq 'Bridge AOR' " \
                      "or OriginatingSystemName eq 'DELTA' " \
                      "or OriginatingSystemName eq 'WEST CONTRA COSTA' " \
                      "or OriginatingSystemName eq 'CONTRA COSTA')".format(since_time, to_time)
        if service_flag == 'open_house':
            headers['Authorization'] = 'Bearer 99a19235ed032619b2bb94bc33fb8fb0'
            request = "https://api.bridgedataoutput.com/api/v2/OData/beccar/OpenHouse?" \
                      "$filter=" \
                      "BridgeModificationTimestamp ge {} and BridgeModificationTimestamp le {}".format(since_time,
                                                                                                       to_time)
    elif provider == 'crmls':
        if service_flag == 'open_house':
            headers = {}
            request = "https://pt.rets.crmls.org/contact/rets/search?SearchType=OpenHouse" \
                      "&Class=OpenHouse" \
                      "&Query=(ModificationTimestamp={}-{})" \
                      "&Count=1" \
                      "&Format=COMPACT-DECODED".format(since_time, to_time)
        else:
            selected_columns = get_provider_query_fields(provider, service_flag=service_flag)
            headers = {}
            request = "https://pt.rets.crmls.org/contact/rets/search?SearchType=Property&Class=CrossProperty" \
                      "&Query=(ModificationTimestamp={}-{})," \
                      "(PhotosCount=1%2b)," \
                      "(PropertyType=Resi,Rinc)," \
                      "(StandardStatus=A)," \
                      "(ListPrice=30000%2b)" \
                      "&Limit={}" \
                      "&Count=1" \
                      "&Offset={}" \
                      "&Select={}" \
                      "&Format=COMPACT-DECODED&Limit=500&Count=1" \
                      "&$expand=Media($filter=MediaCategory has ResourceEnums.MediaCategory'Photo')".format(since_time,
                                                                                                            to_time,
                                                                                                            limit,
                                                                                                            offset,
                                                                                                            selected_columns)
    elif provider == 'sdmls':
        if service_flag == 'open_house':
            headers = {}
            request = "https://sdmls-rets.connectmls.com/server/search?Format=COMPACT-DECODED" \
                      "&Class={}" \
                      "&Limit=100000" \
                      "&RETS-Version=RETS%2F1.7.2" \
                      "&QueryType=DMQL2" \
                      "&Query=(OH_StartDate={}-{})" \
                      "&SearchType=OpenHouse" \
                      "&Count=1".format(class_option, since_time, to_time)
        elif service_flag == 'property':
            selected_columns = get_provider_query_fields(provider, class_option=class_option, service_flag=service_flag)
            headers = {}
            request = "https://sdmls-rets.connectmls.com/server/login/search?SearchType=Property" \
                      "&Class={}" \
                      "&Query=(L_UpdateDate={}-{})," \
                      "(L_SystemPrice=30000%2b)," \
                      "(LM_char10_75= SDR,SDC)" \
                      "&RETS-Version=RETS/1.7.2" \
                      "&QueryType=DMQL2" \
                      "&Format=COMPACT-DECODED" \
                      "&Count=1" \
                      "&Limit={}" \
                      "&Offset={}" \
                      "&Select={}".format(class_option, since_time, to_time, limit, offset, selected_columns)
        elif service_flag == 'agents':
            selected_columns = get_provider_query_fields(provider, class_option=class_option, service_flag=service_flag)
            headers = {}
            request = "https://sdmls-rets.connectmls.com/server/search?SearchType=ActiveAgent" \
                      "&Class=ActiveAgent" \
                      "&Query=(U_UpdateDate={}-{})" \
                      "&Format=COMPACT-DECODED" \
                      "&Count=1" \
                      "&Limit={}" \
                      "&Offset={}" \
                      "&Select={}" \
                      "&QueryType=DMQL2".format(since_time, to_time, limit, offset, selected_columns)
    return headers, request


def get_rets_login_info(provider: str):
    url = aws_secrets.get('qa-comp-proxy').get('mls_credentials').get(provider).get('url')
    user_name = aws_secrets.get('qa-comp-proxy').get('mls_credentials').get(provider).get('user_name')
    password = aws_secrets.get('qa-comp-proxy').get('mls_credentials').get(provider).get('password')
    return url, user_name, password


def do_rets_login(provider: str):
    url, user_name, password = get_rets_login_info(provider)
    if provider == 'crmls':
        try:
            key_session = 'JSESSIONID'
            key_rets_session = 'RETS-Session-ID'
            res = requests.get(url, auth=HTTPDigestAuth(user_name, password))
        except(Timeout, TooManyRedirects, RequestException) as err:
            print(f'Login request for mls: {provider} was NOT successful')
            raise err
    else:
        try:
            key_session = 'SESSION'
            key_rets_session = 'RETS-Session-ID'
            res = requests.get(url, auth=HTTPBasicAuth(user_name, password))
        except(Timeout, TooManyRedirects, RequestException) as err:
            print(f'Login request for mls: {provider} was NOT successful')
            raise err
    value_session_id = res.cookies[key_session]
    value_RETS_Session_ID = res.cookies[key_rets_session]
    if res.status_code == 200:
        print(f'Login request for mls: {provider} was successful')
    else:
        print(f'Login request for mls: {provider} was NOT  successful')
    return key_session, key_rets_session, value_session_id, value_RETS_Session_ID


def set_api_request(provider, base_req, next_val, expend=None, service_flag=None):
    req_string = ''
    place_holder = None
    if provider == 'mls_listings':
        if service_flag is not None:
            req_string = "{}{}"
            place_holder = (base_req, next_val)
        else:
            req_string = "{}{}{}"
            place_holder = (base_req, next_val, expend)
    elif provider == 'bayeast':
        req_string = "{}{}"
        place_holder = (base_req, next_val)
    return req_string, place_holder


def get_res_for_mls_properties_via_proxy(provider: str, since_time: str, to_time: str, offset: int, class_option: str):
    selected_columns = get_provider_query_fields(provider, class_option)
    res = ''
    try:
        req_body = {
            "provider": provider,
            "clazz": class_option,
            "query": f"(LastModifiedDateTime = {since_time}-{to_time}),(SearchPrice=0+),(Status=|A,C,D,F,H,P,S,U,X,Z)"
                     f"&Select={selected_columns}",
            "searchType": "Property",
            "offset": offset
        }
        print(f'Sending post request to {provider}...')
        res = requests.post('https://api-data-server-proxy-internal.reali.com/property/', json=req_body)
        assert not isinstance(res, str)
        assert res.status_code == 200
        assert 'Active' in res.text
        print(f'Got the response with properties for mls: {provider} with class_option: {class_option}')
    except(AssertionError, Timeout, TooManyRedirects, RequestException):
        print(f'Error in getting the response with properties for mls: {provider}')
    return res


def get_res_for_mls_photos_via_proxy(provider: str, since_time: str, to_time: str, offset: int):
    try:
        req_body = {
            "provider": provider,
            "clazz": "PPIC",
            "query": f"(L_Last_Photo_updt={since_time}-{to_time}),(LO1_board_id=1)",
            "searchType": "Media",
            "offset": offset
        }
        print(f'Sending post request to {provider}...')
        res = requests.post('https://api-data-server-proxy-internal.reali.com/media/', req_body)
        assert res.status_code, 200
        print(f'Got the response with photos for mls: {provider}')
    except(AssertionError, Timeout, TooManyRedirects, RequestException):
        print(f'Error in getting the response with photos for mls: {provider}')
        return False
    return res


def get_res_for_mls_open_house_via_proxy(provider: str, since_time: str, to_time: str, offset: int):
    res = ''
    try:
        req_body = {
            "provider": provider,
            "clazz": "OPEN",
            "query": f"(ModificationTimestamp = {since_time}-{to_time})",
            "searchType": "OpenHouse",
            "offset": offset
        }
        print(f'Sending post request to {provider}...')
        res = requests.post('https://api-data-server-proxy-internal.reali.com/openhouse/', json=req_body)
        assert res.status_code == 200
        print(f'Got the response with open house info for mls: {provider}')
    except(AssertionError, Timeout, TooManyRedirects, RequestException):
        print(f'Error in getting the response with open house info for mls: {provider}')
    return res


def get_res_for_mls_agents_via_proxy(provider: str, since_time: str, to_time: str, offset: int):
    try:
        req_body = {
            "provider": provider,
            "clazz": "MEMB",
            "query": f"(LastModifiedDateTime = {since_time}-{to_time})",
            "searchType": "ActiveAgent",
            "offset": offset
        }
        print(f'Sending post request to {provider}...')
        res = requests.post('https://api-data-server-proxy-internal.reali.com/media/', req_body)
        assert res.status_code, 200
        print(f'Got the response with agent info for mls: {provider}')
    except(AssertionError, Timeout, TooManyRedirects, RequestException):
        print(f'Error in getting the response with agent info for mls: {provider}')
        return False
    return res


def get_res_for_mls_office_via_proxy(provider: str, since_time: str, to_time: str, offset: int):
    try:
        req_body = {
            "provider": provider,
            "clazz": "OFFI",
            "query": f"(LastModifiedDateTime = {since_time}-{to_time})",
            "searchType": "Office",
            "offset": offset
        }
        print(f'Sending post request to {provider}...')
        res = requests.post('https://api-data-server-proxy-internal.reali.com/media/', req_body)
        assert res.status_code, 200
        print(f'Got the response with office info for mls: {provider}')
    except(AssertionError, Timeout, TooManyRedirects, RequestException):
        print(f'Error in getting the response with office info for mls: {provider}')
        return False
    return res


def query_common_for_valid_listings_with_modification_time_stamp(since_time_iso, to_time_iso, vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT distinct fls.mlsid, fls.listingkeynumeric, fls.agentkeynumeric from fact_listings fls " \
                "where fls.realihousestatus in ('Active', 'Pending')" \
                "and fls.modificationtimestamp > '{}' and fls.modificationtimestamp < '{}' " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id={})t".format(since_time_iso, to_time_iso, vendor_data_id)
    postg_res = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res


def query_common_for_valid_listings(vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT distinct fls.mlsid, fls.realihousestatus from fact_listings fls " \
                "where fls.realihousestatus not in ('UNKNOWN', 'Sold') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id={})t".format(vendor_data_id)
    postg_res = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res


def query_common_for_valid_listings_with_listings_key_numeric(vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT distinct fls.mlsid ,fls.listingkeynumeric from fact_listings fls " \
                "where fls.realihousestatus in ('Active', 'Pending') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id={})t".format(vendor_data_id)
    postg_res = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res


def query_common_for_cities():
    city_query = "select row_to_json (t) from (" \
                 "select input_city, real_city from dim_cities dc " \
                 "where dc.is_enabled)t"
    city_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, city_query)
    return city_res_dict


def query_common_for_missing(tuple_mis_list, vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT fls.mlsid, realihousestatus, is_duplicated, is_filter, housetype, city, listingprice, comp_proxy_status, zipcode from fact_listings fls " \
                "where fls.mlsid in {} " \
                "and fls.provider_id={})t".format(tuple_mis_list, vendor_data_id)
    postg_res_miss_dict = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res_miss_dict


def query_common_for_agent_and_office(vendor_data_id: str):
    mls_query = "select row_to_json (t) from (" \
                "select * from (" \
                "SELECT distinct fls.mlsid, fls.provider_id , da.memberfullname , COALESCE(da.memberpreferredphone, '') as memberpreferredphone, COALESCE(da.memberemail, '') as memberemail, da.memberstatelicense , COALESCE(do2.officename, '') as officename, do2.officephone from fact_listings fls " \
                "join dim_agents da on fls.agentkeynumeric = da.memberkeynumeric " \
                "and fls.provider_id = da.provider_id " \
                "and da.is_active " \
                "join dim_office do2 on fls.officekeynumeric = do2.officekeynumeric " \
                "and fls.provider_id = do2.provider_id " \
                "and do2.is_active " \
                "where fls.realihousestatus in ('Active', 'Pending') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id in ('3','4','5','7','9') " \
                "union all " \
                "select distinct fls.mlsid, fls.provider_id , fls.agentfullname , COALESCE(fls.agentpreferredphone, '') as agentpreferredphone , COALESCE(fls.agentemail, '') as agentemail , fls.agentstatelicense, COALESCE(fls.officename, '') as officename, fls.officephone from fact_listings fls " \
                "where fls.realihousestatus in ('Active', 'Pending') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id in ('1','2')) as a where a.provider_id = {})t".format(vendor_data_id)
    postg_res = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res


def query_common_for_agents_info():
    agents_query = "select row_to_json (t) from (" \
                   "select memberfullname, memberemail, memberpreferredphone , " \
                   "memberstatelicense , officename " \
                   "from dim_agents da" \
                   "where is_active)t"
    agents_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, agents_query)
    return agents_res_dict


def query_dim_agents_with_fact_key_numeric(list_key_numeric):
    updated_list_key_numeric = []
    for val in list_key_numeric:
        if val is not None:
            updated_list_key_numeric.append(val)
    tuple_key_numeric = tuple(updated_list_key_numeric)
    agents_query = "select row_to_json (t) from (" \
                   "select distinct memberkeynumeric, memberfullname, memberemail, memberpreferredphone , " \
                   "memberstatelicense , officename " \
                   "from dim_agents da " \
                   "where is_active " \
                   "and memberkeynumeric in {})t".format(tuple_key_numeric)
    agents_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, agents_query)
    return agents_res_dict


def query_common_for_dim_photos(vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT fls.mlsid, fls.provider_id , count(dps.mediaurl)from fact_listings fls " \
                "left join dim_photos dps on (fls.listingkeynumeric=dps.resourcerecordkey and fls.provider_id = dps.provider_id) " \
                "where fls.realihousestatus in ('Active', 'Pending') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated " \
                "and not is_filter " \
                "and zipcode is not null " \
                "and mediastatus='Valid' " \
                "and dps.sendtocompproxy=200 " \
                "and fls.internetentirelistingdisplay " \
                "and fls.provider_id={} " \
                "group by fls.mlsid, fls.provider_id)t".format(vendor_data_id)
    postg_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res_dict


def query_common_for_dim_open_house(vendor_data_id):
    mls_query = "select row_to_json (t) from (" \
                "SELECT fls.mlsid, fls.provider_id, count(do2.listingid) from fact_listings fls " \
                "join dim_openhouse do2 on fls.listingkeynumeric  = do2.listingkey and do2.provider_id =  fls.provider_id " \
                "where fls.realihousestatus in ('Active', 'Pending') " \
                "and housetype != 'UNSUPPORTED' " \
                "and not is_duplicated and not is_filter " \
                "and zipcode is not null " \
                "and fls.provider_id={} " \
                "and fls.internetentirelistingdisplay " \
                "and do2.openhousedate is not null and do2.openhousestarttime is not null and do2.openhouseendtime is not null " \
                "group by fls.mlsid, fls.provider_id)t".format(vendor_data_id)
    postg_res = postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
    return postg_res


def query_photos_count_common(vendor, tuple_list_mls_id):
    photos_count_query = "select row_to_json (t) from (" \
                         "select fact_listings.mlsid , fact_listings.provider_id, a.resourcerecordkey, a.pc  from ( " \
                         "select resourcerecordkey , count(*) as pc from dim_photos " \
                         "where provider_id = {} and resourcerecordkey in " \
                         "(select listingkeynumeric from fact_listings where fact_listings.provider_id  = {} and fact_listings.mlsid in {}) " \
                         "group by resourcerecordkey) a " \
                         "join fact_listings on a.resourcerecordkey = fact_listings.listingkeynumeric)t".format(vendor,
                                                                                                                vendor,
                                                                                                                tuple_list_mls_id)
    postg_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, photos_count_query)
    return postg_res_dict


def query_dim_open_house_with_valid_listings(tuple_listing_key_numeric):
    open_house_query = "select row_to_json (t) from (" \
                       "select openhousekey, openhousedate, openhousestarttime, openhouseendtime, listingid, modificationtimestamp from dim_openhouse oh " \
                       "where oh.listingkeynumeric in {})t".format(tuple_listing_key_numeric)
    postg_res_dict = postgress_class.get_query_psg(DB_DATA_COMMON, open_house_query)
    return postg_res_dict


def parse_proxy_res(vendor, res, class_option, service_flag):
    count = 0
    data_dict = {}
    res_json = xmltodict.parse(res.text)
    if service_flag == 'property':
        count, data_dict = parse_res_properties_with_params(res_json, class_option)
    elif service_flag == 'agents':
        count, data_dict = parse_res_agents_with_params(res_json)
    elif service_flag == 'open_house':
        data_dict = parse_res_open_house_events_with_params(vendor, res_json)
    return count, data_dict


def get_provider_query_fields(provider: str, class_option=None, service_flag=None):
    selected_columns = ''
    if provider == 'crmls':
        if service_flag == 'property':
            selected_columns = "LivingArea,LotSizeSquareFeet,StreetName,ListPrice" \
                               ",City,ListingId,PhotosCount,StateOrProvince,StandardStatus," \
                               "PropertySubType,ModificationTimestamp,PostalCode,ListAgentKeyNumeric"
        elif service_flag == 'agents':
            selected_columns = "AgentFullName,AgentEmail,AgentPreferredPhone,AgentStateLicense,officeName"
    if provider == 'sdmls':
        if service_flag == 'property':
            if class_option == 'RE_1':
                selected_columns = "LM_Int4_1,LM_Int4_6,L_Address,L_AskingPrice" \
                                   ",L_City,L_DisplayId,L_PictureCount,L_State,L_Status," \
                                   "L_Type_,L_UpdateDate,L_Zip,L_ListAgent1"
            else:
                selected_columns = "LM_Int4_20,LM_Int4_22,LM_Int4_23,LM_Int4_24,LM_Int4_6,L_Address,L_AskingPrice" \
                                   ",L_City,L_DisplayId,L_PictureCount,L_State,L_Status," \
                                   "L_Type_,L_UpdateDate,L_Zip,L_ListAgent1"
        elif service_flag == 'agents':
            selected_columns = "U_UserFirstName,U_UserMI,U_UserLastName,U_Email,U_PhoneNumber1," \
                               "U_AgentLicenseID,AO_OrganizationName"
    if provider == 'sfar':
        if class_option == 'RESI':
            selected_columns = "SquareFootage,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               ",PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
        else:
            selected_columns = "INCPU1SF,INCPU2SF,INCPU3SF,INCPU4SF,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               ",PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
    if provider == 'metrolist':
        if class_option == 'RESI':
            selected_columns = "SquareFootage,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               ",PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
        else:
            selected_columns = "INCPU1SF,INCPU2SF,INCPU3SF,INCPU4SF,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               ",PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
    if provider == 'baries':
        if class_option == 'RESI':
            selected_columns = "SquareFootage,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               "ListingOfficeNumber,PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
        else:
            selected_columns = "INCPU1SF,INCPU2SF,INCPU3SF,INCPU4SF,LotSquareFootage,StreetName,ListingPrice" \
                               ",City,ListingNumberDisplay,PictureCount,State,Status" \
                               ",PropertySubtype1,LastModifiedDateTime,ZipCode,ListingAgentNumber"
    return selected_columns


def get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso, to_time_iso, class_option=None, service_flag=None):
    offset = 0
    counter = 0
    dict_data = {}
    total_count = 0
    res = ''
    value = ''
    try:
        if service_flag == 'property':
            res = get_res_for_mls_properties_via_proxy(vendor, since_time_iso, to_time_iso, offset, class_option)
        elif service_flag == 'agents':
            res = get_res_for_mls_agents_via_proxy(vendor, since_time_iso, to_time_iso, offset)
        elif service_flag == 'open_house':
            res = get_res_for_mls_open_house_via_proxy(vendor, since_time_iso, to_time_iso, offset)
        assert not isinstance(res, str)
        while res.status_code != 200 and counter == 0:
            env_vars.logger.warning('Failed to get valid response:')
            print('Failed to get valid response:')
            print('Retrying to get response...')
            if service_flag == 'property':
                res = get_res_for_mls_properties_via_proxy(vendor, since_time_iso, to_time_iso, offset, class_option)
                value = 'Active'
            elif service_flag == 'agents':
                res = get_res_for_mls_agents_via_proxy(vendor, since_time_iso, to_time_iso, offset)
                value = 'Phone'
            elif service_flag == 'open_house':
                res = get_res_for_mls_open_house_via_proxy(vendor, since_time_iso, to_time_iso, offset)
                value = 'OpenHouseRid'
            counter += 1
        if res.status_code == 200 and value in res.text:
            total_count, dict_data = parse_proxy_res(vendor, res, class_option, service_flag)
        else:
            env_vars.logger.critical('Failed to get valid response:\n{}'.format(res))
            print('Failed to get valid response:\n')
    except AssertionError:
        print(f'Error in getting the response with properties for mls: {vendor}')
    return total_count, dict_data


def get_res_and_build_dict_for_rets_mls(vendor, since_time_iso, to_time_iso, limit, class_option=None,
                                        service_flag=None):
    offset = 0
    key_session, key_rets_session, value_session_id, value_RETS_Session_ID = do_rets_login(vendor)
    if class_option is not None:
        if service_flag == 'open_house':
            if class_option == 'RI_2':
                class_option = 'RI_4'
        request = base_request(vendor, since_time_iso, to_time_iso, limit, offset, class_option=class_option,
                               service_flag=service_flag)
    else:
        request = base_request(vendor, since_time_iso, to_time_iso, limit, offset, service_flag=service_flag)
    request = request[1]  # for crmls and sandicor does not need headers
    cookies = {key_session: value_session_id, key_rets_session: value_RETS_Session_ID}
    if service_flag == 'open_house':
        dict_data = get_rets_mls_listing_api(request, vendor, service_flag=service_flag, cookies=cookies)
        return dict_data
    else:
        try:
            if class_option is not None:
                total_count, dict_data = get_rets_mls_listing_api(request, vendor, class_option=class_option,
                                                                  service_flag=service_flag, cookies=cookies)
                print(f'Found {total_count} items')
                env_vars.total_properties_count_in_source_per_mls = int(total_count)
            else:
                total_count, dict_data = get_rets_mls_listing_api(request, vendor, cookies=cookies,
                                                                  service_flag=service_flag)
                env_vars.total_properties_count_in_source_per_mls = int(total_count)
                print(f'Found {total_count} items')
            if len(dict_data) == 0:
                env_vars.logger.critical('Failed to get valid response:\n{}'.format(request))
                return False
            while int(total_count) >= offset:
                offset += limit
                if class_option is not None:
                    request = base_request(vendor, since_time_iso, to_time_iso, limit, offset,
                                           class_option=class_option, service_flag=service_flag)
                else:
                    request = base_request(vendor, since_time_iso, to_time_iso, limit, offset,
                                           service_flag=service_flag)
                request = request[1]  # for crmls and sandicor does not need headers
                if offset >= int(total_count):
                    break
                if class_option is not None:
                    count, dict_data = get_rets_mls_listing_api(request, vendor, class_option=class_option,
                                                                service_flag=service_flag, cookies=cookies)
                else:
                    count, dict_data = get_rets_mls_listing_api(request, vendor, cookies=cookies,
                                                                service_flag=service_flag)
        except(Timeout, TooManyRedirects, RequestException) as err:
            print(f'Error in getting response for: {vendor}')
            raise err
    return dict_data


def combine_dict(dict_data_resi, dict_data_incp):
    keys_list = dict_data_incp.keys()
    values_list = dict_data_incp.values()
    val_list = []
    i = 0
    for value in values_list:
        val_list.append(value)
    for key in keys_list:
        dict_data_resi[key] = val_list[i]
        i += 1
    return dict_data_resi


def compare_mls_agg_res_vs_common_dim_agents_dicts(postg_agents_dict, mls_agg_res_agent):
    mis_list_in_dim_agent = check_results_class.which_not_in(not_in_here=postg_agents_dict, from_here=mls_agg_res_agent)
    plus_list_in_dim_agent = check_results_class.which_not_in(not_in_here=mls_agg_res_agent,
                                                              from_here=postg_agents_dict)
    return mis_list_in_dim_agent, plus_list_in_dim_agent


def get_connection_strings(envin):
    global DB_MONGO, DB_DATA_COMMON, DB_DATA_MLS_LISTINGS, aws_secrets
    services = ['qa-comp-proxy', 'af-common-service', 'comp-proxy']
    aws_secrets = get_set_secrerts(envin, services)
    DB_MONGO = get_db_mongo_config(envin, aws_secrets)
    DB_DATA_COMMON = set_postgress_db_from_secret_jdbc('af-common-service', db_source='common', aws_secrets=aws_secrets)
    DB_DATA_MLS_LISTINGS = set_postgress_db_from_secret_jdbc('af-common-service', db_source='mlslisting',
                                                             aws_secrets=aws_secrets)


def build_agg_res_agents_from_dim_agents(agg_res_properties):
    agg_res_agents = {}
    miss_key_numeric_info = []
    key_numeric_list = build_key_numeric_list(agg_res_properties)
    agents_res_dict = query_dim_agents_with_fact_key_numeric(key_numeric_list)
    for item in agents_res_dict:
        if item[0].get('memberkeynumeric') not in key_numeric_list:
            miss_key_numeric_info.append(item[0].get('memberkeynumeric'))
    for value in agents_res_dict:
        member_key_numeric = value[0].get('memberkeynumeric')
        member_full_name = value[0].get('memberfullname')
        member_email = value[0].get('memberemail')
        member_preferred_phone = value[0].get('memberpreferredphone')
        member_state_license = value[0].get('memberstatelicense')
        member_office_name = value[0].get('officename')
        agg_res_agents[member_key_numeric] = {'memberstatelicense': member_state_license,
                                              'memberfullname': member_full_name,
                                              'memberemail': member_email,
                                              'memberpreferredphone': member_preferred_phone,
                                              'officename': member_office_name}
    return agg_res_agents, miss_key_numeric_info


def build_key_numeric_list(agg_res_properties):
    key_numeric_list = []
    res = agg_res_properties.values()
    for item in res:
        value = item[0].get('ListingAgentKeyNumeric')
        key_numeric_list.append(value)
    return key_numeric_list


def validate_photos_count_mls_vs_common(vendor, agg_res_properties):
    mls_photos_count_dict = {}
    common_photos_count_dict = {}
    dict_values = agg_res_properties.values()
    dict_keys = agg_res_properties.keys()
    for value in dict_values:
        photos_count = value[0].get('PhotosCount')
        for mls_id in dict_keys:
            mls_photos_count_dict[mls_id] = photos_count
    tuple_list_mls_id = tuple(dict_keys)
    postg_res_dict = query_photos_count_common(vendor, tuple_list_mls_id)
    for res in postg_res_dict:
        photos_count = res[0].get('pc')
        mls_id = res[0].get('mlsid')
        common_photos_count_dict[mls_id] = photos_count
    miss_list_photos_count = check_results_class.which_not_in(not_in_here=common_photos_count_dict,
                                                              from_here=mls_photos_count_dict)
    return miss_list_photos_count


def update_postg_res_dict(vendor, postg_res_dict, tuple_mls_id_list):
    photos_count_postg_res = query_photos_count_common(vendor, tuple_mls_id_list)
    for res in photos_count_postg_res:
        mls_id = res[0].get('mlsid')
        photos_count = res[0].get('pc')
        if postg_res_dict.get(mls_id) is not None:
            postg_res_dict.get(mls_id).update({'PhotosCount': photos_count})
    return postg_res_dict


def build_agg_res_for_open_house_from_mls_via_proxy(vendor, since_time_iso, to_time_iso, service_flag):
    count, agg_res_open_house = get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso, to_time_iso,
                                                                     service_flag=service_flag)
    return agg_res_open_house


def build_agg_res_for_open_house_from_mls_via_rets(vendor, since_time_iso, to_time_iso, limit, class_option=None,
                                                   service_flag=None):
    agg_res_open_house = get_res_and_build_dict_for_rets_mls(vendor, since_time_iso, to_time_iso, limit, class_option,
                                                             service_flag)
    return agg_res_open_house


def parse_res_open_house_events(res_json, vendor):
    attribute_keys = {'mls_listings': {'openhousekey': 'OpenHouseKeyNumeric', 'listingid': 'ListingId',
                                       'openhousestarttime': 'OpenHouseStartTime',
                                       'openhouseendtime': 'OpenHouseEndTime'},
                      'bayeast': {'openhousekey': 'OpenHouseKey', 'listingid': 'ListingId',
                                  'openhousestarttime': 'BECCAR_OH_StartDateTime',
                                  'openhouseendtime': 'BECCAR_OH_EndDateTime'}}
    dict_attributes = attribute_keys[vendor]
    agg_res_open_house = {}
    for res in res_json.get('value'):
        open_house_key = res.get(dict_attributes['openhousekey'])
        listingid = res.get(dict_attributes['listingid'])
        open_house_start_time = res.get(dict_attributes['openhousestarttime'])
        open_house_end_time = res.get(dict_attributes['openhouseendtime'])
        if agg_res_open_house.get(listingid) is None:
            agg_res_open_house[listingid] = [{'openhousekey': open_house_key,
                                              'openhousestarttime': open_house_start_time,
                                              'openhouseendtime': open_house_end_time}]
        else:
            agg_res_open_house[listingid].append({'openhousekey': open_house_key,
                                                  'openhousestarttime': open_house_start_time,
                                                  'openhouseendtime': open_house_end_time})
    return agg_res_open_house


def build_agg_res_for_open_house_from_mls_via_rest_api(vendor, since_time_iso, to_time_iso, limit, offset,
                                                       service_flag):
    agg_res_open_house = {}
    headers, req = base_request(vendor, since_time_iso, to_time_iso, limit, offset, service_flag=service_flag)
    res_json = get_mls_listing_api(req, headers)
    if len(res_json) == 0:
        print('Error in get response from provider')
        env_vars.logger.critical('Error in get response from provider')
        return agg_res_open_house
    agg_res_open_house = parse_res_open_house_events(res_json, vendor)
    return agg_res_open_house


def build_tuple_listing_key_numeric(postg_res):
    listing_key_numeric_list = []
    for res in postg_res:
        listing_key_numeric = res[0].get('listingkeynumeric')
        listing_key_numeric_list.append(listing_key_numeric)
    tuple_listing_key_numeric = tuple(listing_key_numeric_list)
    return tuple_listing_key_numeric


def build_postg_res_for_open_house(vendor, since_time_iso, to_time_iso):
    if not isinstance(vendor, str):
        vendor_data_id = vendor.get('provider_id')
    else:
        vendor_data_id = provider_name_to_id[vendor].get('provider_id')
    postg_res = query_common_for_valid_listings_with_listings_key_numeric(vendor_data_id)
    tuple_listing_key_numeric = build_tuple_listing_key_numeric(postg_res)
    postg_res_open_house_events = query_dim_open_house_with_valid_listings(tuple_listing_key_numeric)
    postg_res_open_house_events = build_postg_res_as_list_of_dicts(postg_res_open_house_events)
    return postg_res_open_house_events, tuple_listing_key_numeric


def build_postg_res_as_list_of_dicts(postg_res_open_house_events):
    dict_data = {}
    for item in postg_res_open_house_events:
        if dict_data.get(item[0].get('listingid')) is None:
            dict_data[item[0].get('listingid')] = [{'openhousekey': item[0].get('openhousekey'),
                                                    'openhousestarttime': item[0].get('openhousestarttime'),
                                                    'openhouseendtime': item[0].get('openhouseendtime')}]
        else:
            dict_data[item[0].get('listingid')].append({'openhousekey': item[0].get('openhousekey'),
                                                        'openhousestarttime': item[0].get('openhousestarttime'),
                                                        'openhouseendtime': item[0].get('openhouseendtime')})
    return dict_data


def build_postg_res_for_valid_listings_with_modification_time_stamp(since_time_iso, to_time_iso, vendor_data_id):
    postg_res_dict = {}
    postg_res = query_common_for_valid_listings_with_modification_time_stamp(since_time_iso, to_time_iso,
                                                                             vendor_data_id)
    list_mls_id = []
    for res in postg_res:
        property_id = res[0].get('mlsid')
        photos_count = res[0].get('count', 'NA')
        list_key_numeric = res[0].get('agentkeynumeric', 'NA')
        list_mls_id.append(res[0].get('mlsid'))
        postg_res_dict[property_id] = {'PhotosCount': photos_count, 'ListingAgentKeyNumeric': list_key_numeric}
    return postg_res_dict, list_mls_id


def print_summery_result_per_mls(vendor, final_mis_list, duplicated_list, mis_city_list, not_valid_zipcode_list,
                                 not_valid_comps_proxy_status_list, not_valid_house_type_list,
                                 not_valid_listing_price_list,
                                 is_filter_list, not_valid_reali_house_status_list, list_miss_photos_count,
                                 miss_key_numeric_info):
    print(f'Results for the test run on mls {vendor.get("compsproxy_vendortype")}'
          f'\nTotal in source: {env_vars.total_properties_count_in_source_per_mls}'
          f'\nfinal_mis_list: {final_mis_list}'
          f'\nduplicated_list: {duplicated_list}'
          f'\nmis_city_list: {mis_city_list}'
          f'\nnot_valid_zipcode_list: {not_valid_zipcode_list}'
          f'\nnot_valid_comps_proxy_status_list: {not_valid_comps_proxy_status_list}'
          f'\nnot_valid_house_type_list: {not_valid_house_type_list}'
          f'\nnot_valid_listing_price_list: {not_valid_listing_price_list}'
          f'\nis_filter_list: {is_filter_list}'
          f'\nnot_valid_reali_house_status_list: {not_valid_reali_house_status_list}'
          f'\nmiss_list_photos_count: {list_miss_photos_count}'
          f'\nmiss_key_numeric_info_list: {miss_key_numeric_info}\n\n')
    env_vars.logger.critical(f'Results for the test run on mls {vendor.get("compsproxy_vendortype")}'
                             f'\nTotal in source: {env_vars.total_properties_count_in_source_per_mls}'
                             f'\nfinal_mis_list: {len(final_mis_list)}'
                             f'\nduplicated_list: {len(duplicated_list)}'
                             f'\nmis_city_list: {len(mis_city_list)}'
                             f'\nnot_valid_zipcode_list: {len(not_valid_zipcode_list)}'
                             f'\nnot_valid_comps_proxy_status_list: {len(not_valid_comps_proxy_status_list)}'
                             f'\nnot_valid_house_type_list: {len(not_valid_house_type_list)}'
                             f'\nnot_valid_listing_price_list: {len(not_valid_listing_price_list)}'
                             f'\nis_filter_list: {len(is_filter_list)}'
                             f'\nnot_valid_reali_house_status_list: {len(not_valid_reali_house_status_list)}'
                             f'\nmiss_list_photos_count: {len(list_miss_photos_count)}'
                             f'\nmiss_key_numeric_info_list: {len(miss_key_numeric_info)}\n\n')
    env_vars.total_sum_final_mis_list += len(final_mis_list)
    env_vars.total_sum_duplicated_list += len(duplicated_list)
    env_vars.total_sum_mis_city_list += len(mis_city_list)
    env_vars.total_sum_not_valid_zipcode_list += len(not_valid_zipcode_list)
    env_vars.total_sum_not_valid_comps_proxy_status_list += len(not_valid_comps_proxy_status_list)
    env_vars.total_sum_not_valid_house_type_list += len(not_valid_house_type_list)
    env_vars.total_sum_not_valid_listing_price_list += len(not_valid_listing_price_list)
    env_vars.total_sum_is_filter_list += len(is_filter_list)
    env_vars.total_sum_not_valid_reali_house_status_list += len(not_valid_reali_house_status_list)
    env_vars.total_sum_list_miss_photos_count += len(list_miss_photos_count)
    env_vars.total_sum_miss_key_numeric_info += len(miss_key_numeric_info)
    env_vars.total_properties_count_in_source += env_vars.total_properties_count_in_source_per_mls
    env_vars.total_properties_count_in_source_per_mls = 0


def print_total_summery_result():
    if env_vars.total_sum_final_mis_list > 0 \
            or env_vars.total_sum_duplicated_list > 0 \
            or env_vars.total_sum_mis_city_list > 0 \
            or env_vars.total_sum_not_valid_zipcode_list > 0 \
            or env_vars.total_sum_not_valid_comps_proxy_status_list > 0 \
            or env_vars.total_sum_not_valid_listing_price_list > 0 \
            or env_vars.total_sum_is_filter_list > 0 \
            or env_vars.total_sum_not_valid_reali_house_status_list > 0 \
            or env_vars.total_sum_list_miss_photos_count > 0 \
            or env_vars.total_sum_miss_key_numeric_info > 0 \
            or env_vars.total_sum_not_valid_house_type_list > 0:
        env_vars.logger.critical('Results for the test run on ALL OF THE MLS:'
                                 f'\nTotal properties in source is: {env_vars.total_properties_count_in_source}'
                                 f'\nfinal_mis_list: {env_vars.total_sum_final_mis_list}'
                                 f'\nduplicated_list: {env_vars.total_sum_duplicated_list}'
                                 f'\nmis_city_list: {env_vars.total_sum_mis_city_list}'
                                 f'\nnot_valid_zipcode_list: {env_vars.total_sum_not_valid_zipcode_list}'
                                 f'\nnot_valid_comps_proxy_status_list: {env_vars.total_sum_not_valid_comps_proxy_status_list}'
                                 f'\nnot_valid_house_type_list: {env_vars.total_sum_not_valid_house_type_list}'
                                 f'\nnot_valid_listing_price_list: {env_vars.total_sum_not_valid_listing_price_list}'
                                 f'\nis_filter_list: {env_vars.total_sum_is_filter_list}'
                                 f'\nnot_valid_reali_house_status_list: {env_vars.total_sum_not_valid_reali_house_status_list}'
                                 f'\nmiss_list_photos_count: {env_vars.total_sum_list_miss_photos_count}'
                                 f'\nmiss_key_numeric_info_list: {env_vars.total_sum_miss_key_numeric_info}\n\n')
    else:
        env_vars.logger.info('Results for the test run on ALL OF THE MLS:'
                             f'\nTotal properties in source is: {env_vars.total_properties_count_in_source}'
                             f'\nfinal_mis_list: {env_vars.total_sum_final_mis_list}'
                             f'\nduplicated_list: {env_vars.total_sum_duplicated_list}'
                             f'\nmis_city_list: {env_vars.total_sum_mis_city_list}'
                             f'\nnot_valid_zipcode_list: {env_vars.total_sum_not_valid_zipcode_list}'
                             f'\nnot_valid_comps_proxy_status_list: {env_vars.total_sum_not_valid_comps_proxy_status_list}'
                             f'\nnot_valid_house_type_list: {env_vars.total_sum_not_valid_house_type_list}'
                             f'\nnot_valid_listing_price_list: {env_vars.total_sum_not_valid_listing_price_list}'
                             f'\nis_filter_list: {env_vars.total_sum_is_filter_list}'
                             f'\nnot_valid_reali_house_status_list: {env_vars.total_sum_not_valid_reali_house_status_list}'
                             f'\nmiss_list_photos_count: {env_vars.total_sum_list_miss_photos_count}'
                             f'\nmiss_key_numeric_info_list: {env_vars.total_sum_miss_key_numeric_info}\n\n')


def print_open_house_result_per_mls(vendor, mis_lis, plus_lis):
    print(f'\nResults for the test run of open house events on mls {vendor}:'
          f'\nMiss events list: {mis_lis}\n')
    print(f'\nPlus events list: {plus_lis}\n')
    env_vars.total_sum_mis_lis += len(mis_lis)
    env_vars.total_sum_plus_lis += len(plus_lis)


def print_open_house_total_result():
    if env_vars.total_sum_mis_lis > 0 or env_vars.total_sum_plus_lis > 0:
        env_vars.logger.critical(
            f'\nResults for the test run of open house events on ALL OF THE MLS:'
            f'\nMiss events list: {env_vars.total_sum_mis_lis}\n'
            f'Plus events list: {env_vars.total_sum_plus_lis}\n')
    else:
        env_vars.logger.info(
            f'\nResults for the test run of open house events on ALL OF THE MLS:'
            f'\nMiss events list: {env_vars.total_sum_mis_lis}\n'
            f'Plus events list: {env_vars.total_sum_plus_lis}\n')


def query_mongo_listing_collection(vendor_identifiers):
    query_mongo_for_listing = {
        'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
        'currentListing.listingStatus': {'$in': ['ACTIVE', 'PENDING']},
        'toIndex': True,
        'toDelete': False
    }
    projection = {"item1": 1,
                  "vendorMlsId": "$currentListing.vendorMlsId",
                  "item2": 1,
                  "listingStatus": "$currentListing.listingStatus",
                  "item3": 1,
                  "zipCode": "$location.zipCode",
                  "item4": 1,
                  "inServiceArea": "$location.inServiceArea",
                  }
    full_query = [
        {
            "$match": query_mongo_for_listing
        },
        {"$project": projection
         },
        {
            "$sort": {"realiUpdated": -1}
        }
    ]
    return full_query


def query_mongo_zip_code_coverage_collection(zip_code_list):
    query_mongo_for_zip_code_coverage = {
        'zipcode': {'$in': zip_code_list}
    }
    projection = {"item1": 1,
                  "zipCode": "$zipcode",
                  "item2": 1,
                  "inServiceArea": "$inServiceArea",
                  }
    full_query = [
        {
            "$match": query_mongo_for_zip_code_coverage
        },
        {"$project": projection
         },
        {
            "$sort": {"realiUpdated": -1}
        }
    ]
    return full_query


def print_zip_code_coverage_alignment_result(mis_list, plus_list, vendor):
    env_vars.total_sum_of_zip_code_coverage_of_mis_list += len(mis_list)
    env_vars.total_sum_of_zip_code_coverage_of_plus_list += len(plus_list)
    print(f'Test result for checking alignment between listings and zipCodeCoverage collections in mongo db for {vendor} is: \n'
          f'miss list: {mis_list}\n'
          f'plus list: {plus_list}\n')
    if len(mis_list) > 0 or len(plus_list > 0):
        env_vars.logger.critical(
            f'Test result for checking alignment between listings and zipCodeCoverage collections in mongo db for {vendor} is: \n'
            f'miss list: {len(mis_list)}\n'
            f'plus list: {len(plus_list)}\n')
        res = False
    else:
        env_vars.logger.info(
            f'Test result for checking alignment between listings and zipCodeCoverage collections in mongo db for {vendor} is: \n'
            f'miss list: {len(mis_list)}\n'
            f'plus list: {len(plus_list)}\n')
        res = True
    return res


def print_total_result_for_zip_code_coverage():
    mis_ratio = 100 * (env_vars.total_sum_of_zip_code_coverage_of_mis_list + env_vars.total_sum_of_zip_code_coverage_of_plus_list) / env_vars.len_of_mongo_res_listing_dict
    print(f'Total of Miss from ALL MLS is: {env_vars.total_sum_of_zip_code_coverage_of_mis_list}')
    print(f'Total of Plus from ALL MLS is: {env_vars.total_sum_of_zip_code_coverage_of_plus_list}')
    print(f'Total in source from ALL MLS is: {env_vars.len_of_mongo_res_listing_dict}')
    print(f'Miss alignment ration of zip code coverage is: {mis_ratio}%')
    env_vars.logger.info(f'Total of Miss from ALL MLS is: {env_vars.total_sum_of_zip_code_coverage_of_mis_list}')
    env_vars.logger.info(f'Total of Plus from ALL MLS is: {env_vars.total_sum_of_zip_code_coverage_of_plus_list}')
    env_vars.logger.info(f'Total in source from ALL MLS is: {env_vars.len_of_mongo_res_listing_dict}')
    env_vars.logger.info(f'Miss alignment ration of zip code coverage is: {mis_ratio}%')


@pytest.mark.test_alignment_24
def test_last_24_hours(envin, aws_new):
    """
        1. MLSListing , Bayeast — > rest api
        2. Crmls, Sandicor, —> direct rets
        3. Sfar, Metrolist, Bareis —> proxy service (direct rets)
    """
    since_hours = 24
    grace_minutes = 10
    limit = 500
    offset = 0
    agg_res_properties = None
    agg_res_open_house = None
    if str(aws_new).lower() != 'true':
        aws_new = False
    params.aws_new_account = aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    matplotlib.use('Agg')
    env_vars.env = envin
    start = time.time()
    since_time = datetime.datetime.now() - datetime.timedelta(hours=since_hours)
    since_time_iso = since_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    to_time_iso = (datetime.datetime.now() - datetime.timedelta(minutes=grace_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        get_connection_strings(envin)
        res_acc = []
        env_vars.logger = startLogger(logPath='DataPipes_{}.log'.format(envin))
        env_vars.logger.info(
            '\nThe Test is running on {0} env, comparing Active listings on Source to Active on {1}.\n'
            'The test is sampling the last {2} hours with {3} minutes grace until Now.\n'
            'the result for the missing will be printed by mls and then in total summary,\n'
            'In each print will be a drill down that specify the reason why listing is missing\n'
                .format(envin, 'common db', since_hours, grace_minutes))
        test_type = 'properties_24'
        print('Test run: ', test_type)
        expend = "&$expand=Media($filter=MediaCategory has ResourceEnums.MediaCategory'Photo')"
        city_res_dict = query_common_for_cities()
        cities_dict = {}
        for city in city_res_dict:
            key = city[0].get('input_city')
            value = city[0].get('real_city')
            cities_dict[key] = value
        provider_properties = {}
        for vendor in vendors2_test:
            if not provider_properties.get(vendor):
                provider_properties[vendor] = []
            if vendor in ['mls_listings', 'bayeast']:
                count = 100
                next_skip = 0
                next_val = '&$skip={}&$top={}'.format(next_skip, count)
                headers, base_req = base_request(vendor, since_time_iso, to_time_iso, limit, offset,
                                                 service_flag='property')
                base_req = base_req.format(since_time_iso, to_time_iso)
                req_string, place_holder = set_api_request(vendor, base_req, next_val, expend)
                req = req_string.format(*place_holder)
                print(f'sending request to {vendor}...')
                res_json = get_mls_listing_api(req, headers)
                if len(res_json) == 0:
                    env_vars.logger.critical('Fail to get response from:\n{}'.format(req))
                    break
                agg_res_properties = {}
                data_count = res_json.get('@odata.count')
                env_vars.total_properties_count_in_source_per_mls = int(data_count)
                print('Now Featching {} Results in {} offset steps'.format(data_count, count))
                agg_res_properties = get_resp_properties_and_agent_key_numeric_for_rest_api(res_json,
                                                                                            agg_res_properties)
                while next_skip < data_count + count:
                    next_skip += count
                    provider_properties[vendor] += (res_json.get('value'))
                    next_val = '&$skip={}&$top={}'.format(next_skip, count)
                    req_string, place_holder = set_api_request(vendor, base_req, next_val, expend)
                    req = req_string.format(*place_holder)
                    res_json = get_mls_listing_api(req, headers)
                    if len(res_json) == 0:
                        print('Error in getting response from provider')
                        env_vars.logger.critical('Error in getting response from provider')
                        break
                    agg_res_properties = get_resp_properties_and_agent_key_numeric_for_rest_api(res_json,
                                                                                                agg_res_properties)
                agg_res_open_house = build_agg_res_for_open_house_from_mls_via_rest_api(vendor, since_time_iso,
                                                                                        to_time_iso, limit, offset,
                                                                                        service_flag='open_house')
            if vendor in ['crmls', 'sdmls']:
                class_options = ['RE_1', 'RI_2']
                agg_res_re_1_properties = {}
                agg_res_ri_2_properties = {}
                agg_res_re_1_open_house = {}
                agg_res_ri_2_open_house = {}
                if vendor == 'sdmls':
                    for class_option in class_options:
                        agg_res_properties = get_res_and_build_dict_for_rets_mls(vendor, since_time_iso, to_time_iso,
                                                                                 limit, class_option,
                                                                                 service_flag='property')
                        agg_res_open_house = build_agg_res_for_open_house_from_mls_via_rets(vendor, since_time_iso,
                                                                                            to_time_iso, limit,
                                                                                            class_option,
                                                                                            service_flag='open_house')
                        if class_option == 'RE_1':
                            agg_res_re_1_properties = agg_res_properties
                            agg_res_re_1_open_house = agg_res_open_house
                        else:
                            agg_res_ri_2_properties = agg_res_properties
                            agg_res_ri_2_open_house = agg_res_open_house
                    agg_res_properties = combine_dict(agg_res_re_1_properties, agg_res_ri_2_properties)
                    if len(agg_res_re_1_open_house) == 0:
                        agg_res_open_house = agg_res_ri_2_open_house
                    elif len(agg_res_ri_2_open_house) == 0:
                        agg_res_open_house = agg_res_re_1_open_house
                    else:
                        agg_res_open_house = combine_dict(agg_res_re_1_open_house, agg_res_ri_2_open_house)
                else:
                    agg_res_properties = get_res_and_build_dict_for_rets_mls(vendor, since_time_iso, to_time_iso, limit,
                                                                             service_flag='property')
                    agg_res_open_house = build_agg_res_for_open_house_from_mls_via_rets(vendor, since_time_iso,
                                                                                        to_time_iso, limit,
                                                                                        service_flag='open_house')
            if vendor in ['sfar', 'metrolist', 'bareis']:
                class_options = ['RESI',
                                 'INCP']  # RESI = Residential - Properties feed , INCP = Residential Income - Properties feed
                agg_res_resi_properties = {}
                agg_res_incp_properties = {}
                for class_option in class_options:
                    try:
                        total_count, dict_data = get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso,
                                                                                      to_time_iso, class_option,
                                                                                      service_flag='property')
                        print(f'Found {total_count} items')
                        env_vars.total_properties_count_in_source_per_mls = int(total_count)
                        if len(dict_data) > 0:
                            agg_res_properties = {}
                            agg_res_properties = get_resp_properties_and_agent_key_numeric(dict_data,
                                                                                           agg_res_properties)
                            while int(total_count) >= offset:
                                offset += limit
                                if offset >= int(total_count):
                                    break
                                count, dict_data = get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso,
                                                                                        to_time_iso, class_option,
                                                                                        service_flag='property')
                                agg_res_properties = get_resp_properties_and_agent_key_numeric(dict_data,
                                                                                               agg_res_properties)
                            if class_option == 'RESI':
                                agg_res_resi_properties = agg_res_properties
                            else:
                                agg_res_incp_properties = agg_res_properties
                            offset = 0
                    except(Timeout, TooManyRedirects, RequestException) as err:
                        print(f'Error in getting response for: {vendor}')
                        raise err
                if len(agg_res_resi_properties) > 0 and len(agg_res_incp_properties) > 0:
                    agg_res_properties = combine_dict(agg_res_resi_properties, agg_res_incp_properties)
                agg_res_open_house = build_agg_res_for_open_house_from_mls_via_proxy(vendor, since_time_iso,
                                                                                     to_time_iso,
                                                                                     service_flag='open_house')
            if agg_res_properties is not None:
                vendor = provider_name_to_id.get(vendor)
                vendor_data_id = vendor.get('provider_id')
                postg_res_dict, list_mls_id = build_postg_res_for_valid_listings_with_modification_time_stamp(
                    since_time_iso, to_time_iso, vendor_data_id)
                tuple_mls_id_list = tuple(list_mls_id)
                postg_res_dict = update_postg_res_dict(vendor_data_id, postg_res_dict, tuple_mls_id_list)
                agg_res_agents, miss_key_numeric_info = build_agg_res_agents_from_dim_agents(agg_res_properties)
                if len(agg_res_properties) > 0:
                    mis_lis = check_results_class.which_not_in(not_in_here=postg_res_dict,
                                                               from_here=agg_res_properties)
                    plus_lis = check_results_class.which_not_in(not_in_here=agg_res_properties,
                                                                from_here=postg_res_dict)
                    print('Provider: ', vendor.get('compsproxy_vendortype'))
                    print('Miss list: ', mis_lis)
                    print('Plus list: ', plus_lis)
                    tuple_mis_list = tuple(mis_lis)
                    list_miss_photos_count = validate_photos_count_mls_vs_common(vendor_data_id, agg_res_properties)
                    if len(mis_lis) > 0:
                        postg_res_miss_dict = query_common_for_missing(tuple_mis_list, vendor_data_id)
                        final_mis_list, duplicated_list, mis_city_list, not_valid_zipcode_list, \
                        not_valid_comps_proxy_status_list, not_valid_listing_price_list, \
                        not_valid_house_type_list, is_filter_list, \
                        not_valid_reali_house_status_list = \
                            check_results_class.check_mis_res_reason(postg_res_miss_dict, cities_dict)
                        if len(final_mis_list) > 0 or len(duplicated_list) > 0 or len(mis_city_list) > 0 or len(
                                not_valid_zipcode_list) > 0 or len(not_valid_comps_proxy_status_list) > 0 or len(
                            not_valid_listing_price_list) > 0 or len(not_valid_house_type_list) > 0 or len(
                            is_filter_list) > 0 or len(list_miss_photos_count) > 0 or len(miss_key_numeric_info) > 0 \
                                or len(not_valid_reali_house_status_list) > 0:
                            print_summery_result_per_mls(vendor, final_mis_list, duplicated_list, mis_city_list,
                                                         not_valid_zipcode_list, not_valid_comps_proxy_status_list,
                                                         not_valid_listing_price_list, not_valid_house_type_list,
                                                         is_filter_list,
                                                         list_miss_photos_count, miss_key_numeric_info,
                                                         not_valid_reali_house_status_list)
                            res_acc.append(False)
                        else:
                            print(f'No missing listings for provider: {vendor.get("provider_name")}')
                            env_vars.logger.info(f'No missing listings for provider: {vendor.get("provider_name")}')
                    else:
                        print(f'No missing listings for provider: {vendor.get("provider_name")}')
                        env_vars.logger.info(f'No missing listings for provider: {vendor.get("provider_name")}')
                else:
                    print(f'Did not managed to build agg_res dict for: {vendor.get("provider_name")}')
                    env_vars.logger.info(f'Did not managed to build agg_res dict for: {vendor.get("provider_name")}')
            else:
                print('agg_res_properties is None')
                env_vars.logger.warning('agg_res_properties is None')
                res_acc.append(False)
            if len(agg_res_open_house) > 0:
                postg_res_open_house_events, tuple_listing_key_numeric = build_postg_res_for_open_house(vendor,
                                                                                                        since_time_iso,
                                                                                                        to_time_iso)
                mis_lis = check_results_class.which_not_in(not_in_here=postg_res_open_house_events,
                                                           from_here=agg_res_open_house)
                plus_lis = check_results_class.which_not_in(not_in_here=agg_res_open_house,
                                                            from_here=postg_res_open_house_events)
                print_open_house_result_per_mls(vendor, mis_lis, plus_lis)
            else:
                print(f'No open events for open house was found for provider: {vendor}')
                env_vars.logger.critical(f'No open events for open house was found for provider: {vendor}')
                res_acc.append(False)
        print_total_summery_result()
        print_open_house_total_result()
        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))
    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment_common
def test_active_mongo_properties_vs_common_fact(envin, aws_new, vendor):
    cross_env = False
    if str(aws_new).lower() != 'true':
        aws_new = False
    params.aws_new_account = aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    if vendor and vendor != 'None':
        vendors2_test = [vendor]
    else:
        vendors2_test = params.vendors2_test
    start = time.time()
    env_vars.summary = {}
    env_vars.mis_lis = []
    env_vars.plus_lis = []
    env_vars.duplicate_count = 0
    env_vars.postg_res_count = 0
    env_vars.mis_city = []
    env_vars.env = envin
    try:
        env_vars.logger = startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        get_connection_strings(envin)
        res_acc = []
        if cross_env:
            log_message = 'The Test is running on {0} env, comparing Active listings on Mongo Prod to Fact Listing.\nIf a listing was not found Fact Table, a resolution of missing reason will be indicated.\nTest will fail only if number of missing/surplus is greater then {1}.'.format(
                envin, TRASHOLD)
        else:
            log_message = 'The Test is running on {0} env, comparing Active listings on Mongo {0} to Fact Listing.\nIf a listing was not found Fact Table, a resolution of missing reason will be indicated.\nTest will fail only if number of missing/surplus is greater then {1}.'.format(
                envin, TRASHOLD)
        print(log_message)
        env_vars.logger.info(log_message)
        test_type = 'properties'
        print(f'now running test: {test_type}')
        for vendor in vendors2_test:
            postg_res_dict = {}
            mongo_res_photos_count = {}
            mongo_res_photos_count[vendor] = {}
            mongo_res_open_house_count = {}
            mongo_res_open_house_count[vendor] = {}
            mongo_res_expert_info = {}
            mongo_res_expert_info[vendor] = {}
            mongo_res_reali_house_status = {}
            mongo_res_reali_house_status[vendor] = {}
            vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
            db_mongo_args_production_saved = {
                'collection': 'listings',
                'cluster': 'mongodb://realiprod:tfJcU0DCZ3DtgdeM@echo-prod-shard-00-00.pyagu.mongodb.net',
                'port': 27017,
                'dbname': 'realidb-production'}
            if not cross_env:
                db_mongo_args = get_db_mongo_args(envin, 'listings')
                query_mongo = {
                    'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                    'currentListing.listingStatus': {'$in': ['ACTIVE', 'PENDING']},
                    'toIndex': True,
                    'toDelete': False
                }
            else:
                db_mongo_args = db_mongo_args_production_saved
                query_mongo = {'currentListing.listDate': {'$gte': SAMPLE_SINCE},
                               'currentListing.mlsVendorType': vendor_identifiers['compsproxy_vendortype'],
                               'currentListing.listingStatus': {'$in': ['ACTIVE', 'PENDING']},
                               'toIndex': True,
                               'toDelete': False
                               }
            projection = {"item1": 1,
                          "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                          "item2": 1,
                          "assets_details": "$assets",
                          "item3": 1,
                          "vendorMlsId": "$currentListing.vendorMlsId",
                          "item4": 1,
                          "realiUpdated": "$realiUpdated",
                          "item5": 1,
                          "listingStatus": "$currentListing.listingStatus",
                          "listDate": "$currentListing.listDate",
                          "item6": 1,
                          "openHouseEvents": {
                              "$cond": {"if": "$hasOpenHouse", "then": {"$size": "$openHouse"}, "else": 0}},
                          "item7": 1,
                          "expertsInfo": "$expertsInfo",
                          }
            full_query = [
                {
                    "$match": query_mongo
                },
                {"$project": projection
                 },
                {
                    "$sort": {"realiUpdated": -1}
                }
            ]
            mongo_res_dict = {}
            start = time.time()
            connect = None
            mongo_res = mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
            end = time.time()
            print("{} - Mongo - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
            for mres in mongo_res:
                listing_id = mres.get('vendorMlsId')
                if not mongo_res_dict.get(listing_id):
                    mongo_res_photos_count[vendor][listing_id] = mres.get('assets')
                    mongo_res_open_house_count[vendor][listing_id] = mres.get('openHouseEvents')
                    mongo_res_reali_house_status[vendor][listing_id] = mres.get('listingStatus')
                    mongo_res_dict[listing_id] = mres.get('listingStatus')
                    experts_list = mres.get('expertsInfo')
                    if experts_list is not None:
                        for expert in experts_list:
                            if expert.get('type') == 'listing-agent':
                                mongo_res_expert_info[vendor][listing_id] = mres.get('expertsInfo')[0]
                else:
                    mongo_res_dict[listing_id] = mres.get('listingStatus')
            vendor_data_id = vendor_identifiers.get('provider_id')
            postg_res = query_common_for_valid_listings(vendor_data_id)
            for res in postg_res:
                property_id = res[0].get('mlsid')
                photos_count = res[0].get('count', 'NA')
                open_house_count = res[0].get('count', 'NA')
                reali_house_status = res[0].get('realihousestatus')
                postg_res_dict[property_id] = photos_count
                postg_res_dict[property_id] = open_house_count
                postg_res_dict[property_id] = reali_house_status
            env_vars.postg_res_dict[vendor] = postg_res_dict
            env_vars.mongo_res_photos_count[vendor] = mongo_res_photos_count[vendor]
            env_vars.mongo_res_open_house_count[vendor] = mongo_res_open_house_count[vendor]
            env_vars.mongo_res_expert_info[vendor] = mongo_res_expert_info[vendor]
            env_vars.mongo_res_reali_house_status[vendor] = mongo_res_reali_house_status[vendor]
            if not cross_env:
                res, mis_lis = check_results_class.check_res_properties_mongo_postgress_diff_postgress_lead(
                    env_vars.logger, vendor, mongo_res_dict, postg_res_dict, env_vars)
                res_acc.append(res)
            else:
                res, mis_lis = check_results_class.check_res_properties_mongo_postgress_diff(
                    env_vars.logger, vendor, mongo_res_dict, postg_res_dict, env_vars)
                res_acc.append(res)
                if len(mis_lis) > 0:
                    asy_res_fact_specific = get_act_fact_listings(DB_DATA_COMMON, mis_lis)
                    if len(asy_res_fact_specific) > 0:
                        insights = check_results_class.get_act_fact_insights(asy_res_fact_specific)
                        for key, val in insights.items():
                            log_message = '{}: Missing {} marked = {}'.format(vendor, key, len(val))
                            log_message_details = '{}: Missing {} marked = {}\n{}'.format(vendor, key, len(val), val)
                            env_vars.logger.info(log_message)
                            print('{}\n'.format(log_message_details))
                        mis_lis_dict = {}
                        for i in mis_lis:
                            mis_lis_dict[i] = 1
                        not_in_fact = check_results_class.which_not_in(asy_res_fact_specific, mis_lis_dict)
                        if len(not_in_fact) > 0:
                            log_message = '{}: Missing Not in Fact = {}'.format(vendor, len(not_in_fact))
                            log_message_details = '{}: Missing Not in Fact Detailed = {}\n{}'.format(vendor,
                                                                                                     len(not_in_fact),
                                                                                                     not_in_fact)
                            env_vars.logger.info(log_message)
                            print('{}\n'.format(log_message_details))
                    else:
                        env_vars.logger.critical('{}: All missing listings are not in Fact Table'.format(vendor))

        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info, info.tb_frame, info.tb_lineno))

    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment_common
def test_active_mongo_vs_common_dim_photos(envin, aws_new, vendor):
    start = time.time()
    try:
        if vendor and vendor != 'None':
            vendors2_test = [vendor]
        else:
            vendors2_test = params.vendors2_test
        res_acc = []
        cross_env = False
        env_vars.logger.info(
            'The Test is running on {0} env, comparing the expected Number of photos frm dim photos to the actual number on Mongodb\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', TRASHOLD))
        test_type = 'photos'
        print(f'now running the test: {test_type}')
        for vendor in vendors2_test:
            postg_res_dict = {}
            vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
            vendor_data_id = vendor_identifiers.get('provider_id')
            postg_res = query_common_for_dim_photos(vendor_data_id)
            for res in postg_res:
                property_id = res[0].get('mlsid')
                photos_count = res[0].get('count')
                postg_res_dict[property_id] = photos_count
            env_vars.postg_res_dict[vendor] = postg_res_dict
        for vendor in vendors2_test:
            if not cross_env:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values_count(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.mongo_res_photos_count,
                    env_vars)
            else:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values_count(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.postg_res_photos_count, env_vars)
            res_acc.append(res)
        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2].tb_frame, info[2].tb_lineno))
    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment_common
def test_active_mongo_vs_common_dim_open_house(envin, aws_new, vendor):
    start = time.time()
    try:
        if vendor and vendor != 'None':
            vendors2_test = [vendor]
        else:
            vendors2_test = params.vendors2_test
        res_acc = []
        cross_env = False
        env_vars.logger.info(
            'The Test is running on {0} env, comparing the expected Number of open houses frm dim openHouse to the actual number on Mongodb\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', TRASHOLD))
        test_type = 'openHouse'
        print(f'now running the test: {test_type}')
        for vendor in vendors2_test:
            postg_res_dict = {}
            vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
            vendor_data_id = vendor_identifiers.get('provider_id')
            postg_res = query_common_for_dim_open_house(vendor_data_id)
            for res in postg_res:
                property_id = res[0].get('mlsid')
                open_house_count = res[0].get('count')
                postg_res_dict[property_id] = open_house_count
            env_vars.postg_res_dict[vendor] = postg_res_dict
        for vendor in vendors2_test:
            if not cross_env:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values_count(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.mongo_res_open_house_count,
                    env_vars)
            else:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values_count(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.postg_res_open_house_count, env_vars)
            res_acc.append(res)
        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2].tb_frame, info[2].tb_lineno))
    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment_common
def test_active_mongo_vs_common_agent_and_office(envin, aws_new, vendor):
    start = time.time()
    try:
        if vendor and vendor != 'None':
            vendors2_test = [vendor]
        else:
            vendors2_test = params.vendors2_test
        res_acc = []
        cross_env = False
        env_vars.logger.info(
            'The Test is running on {0} env, comparing the agent info from dim_agent and dim_office and fact on Mongodb\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', TRASHOLD))
        test_type = 'agent in office'
        print(f'running test: {test_type}')
        for vendor in vendors2_test:
            postg_res_dict = {}
            vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
            vendor_data_id = vendor_identifiers.get('provider_id')
            postg_res = query_common_for_agent_and_office(vendor_data_id)
            for res in postg_res:
                property_id = res[0].get('mlsid')
                agent_full_name = res[0].get('memberfullname')
                agent_preferred_phone = res[0].get('memberpreferredphone')
                agent_mail = res[0].get('memberemail')
                agent_state_license = res[0].get('memberstatelicense')
                agent_office_name = res[0].get('officename')
                postg_res_dict[property_id] = {'agentName': agent_full_name, 'agentEmail': agent_mail,
                                               'agentPhone': agent_preferred_phone, 'agentBRE': agent_state_license,
                                               'officeName': agent_office_name}
            env_vars.postg_res_dict[vendor] = postg_res_dict
        for vendor in vendors2_test:
            if not cross_env:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.mongo_res_expert_info,
                    env_vars)
            else:
                res, mis_lis, plus_lis, mis_val, plus_val = check_results_class.check_res_properties_dicts_and_values(
                    env_vars.logger, vendor,
                    env_vars.postg_res_dict,
                    env_vars.mongo_res_expert_info, env_vars)
            res_acc.append(res)
        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2].tb_frame, info[2].tb_lineno))
    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment_common
def test_active_mongo_vs_common_summary(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new = False
    params.aws_new_account = aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    start = time.time()
    try:
        res_acc = []
        test_type = 'properties'
        res_acc.append(summary(test_type, envin))
        if False in res_acc:
            res = False
        else:
            res = True
    except():
        res = False
        info = sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2].tb_frame, info[2].tb_lineno))
    end = time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_mls_alignment_dim_agents
def test_mls_vs_common_dim_agent(envin, aws_new, vendor):
    mls_agg_res_agent = {}
    proxy_agg_res_agents = {}
    rest_api_agg_res_agents = {}
    rets_agg_res_agents = {}
    limit = 500
    offset = 0
    grace_minutes = 10
    test_result_flag = False
    if str(aws_new).lower() != 'true':
        aws_new = False
    params.aws_new_account = aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    matplotlib.use('Agg')
    env_vars.env = envin
    since_time_iso = '2020-01-01T00:00:00Z'
    to_time_iso = (datetime.datetime.now() - datetime.timedelta(minutes=grace_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        get_connection_strings(envin)
        env_vars.logger = startLogger(logPath='DataPipes_{}.log'.format(envin))
        env_vars.logger.info('Test mls alignment to dim agents')
        test_type = 'test_mls_alignment_dim_agents'
        print('Test run: ', test_type)
        provider_properties = {}
        vendors2_test = ['mls_listings']
        for vendor in vendors2_test:
            if not provider_properties.get(vendor):
                provider_properties[vendor] = []
        for vendor in vendors2_test:
            if vendor in ['mls_listings', 'bayeast']:
                # mls_listings - direct rest api return all agents info in property request
                # bayeast - direct rest api for table agents different from property request
                count = 30
                next_skip = 0
                next_val = '&$skip={}&$top={}'.format(next_skip, count)
                headers, base_req = base_request(vendor, since_time_iso, to_time_iso, limit, offset,
                                                 service_flag='property')
                base_req = base_req.format(since_time_iso, to_time_iso)
                req_string, place_holder = set_api_request(vendor, base_req, next_val, service_flag='agents')
                req = req_string.format(*place_holder)
                print(f'sending request to {vendor}...')
                res_json = get_mls_listing_api(req, headers)
                if len(res_json) == 0:
                    env_vars.logger.critical('Fail to get response from:\n{}'.format(req))
                    break
                rest_api_agg_res_agents = {}
                rest_api_agg_res_agents['agents_info'] = []
                data_count = res_json.get('@odata.count')
                print('Now Featching {} Results in {} offset steps'.format(data_count, count))
                rest_api_agg_res_agents = build_agg_res_agents(res_json, rest_api_agg_res_agents)
                while next_skip < data_count + count:
                    next_skip += count
                    provider_properties[vendor] += (res_json.get('value'))
                    next_val = '&$skip={}&$top={}'.format(next_skip, count)
                    req_string, place_holder = set_api_request(vendor, base_req, next_val, service_flag='agents')
                    req = req_string.format(*place_holder)
                    res_json = get_mls_listing_api(req, headers)
                    if len(res_json) == 0:
                        print('Error in get response from provider')
                        env_vars.logger.critical('Error in get response from provider')
                        break
                    rest_api_agg_res_agents = build_agg_res_agents(res_json, rest_api_agg_res_agents)
            if vendor in ['sdmls', 'crmls']:
                # for crmls - rets login return all agents info in property request
                # for sdmls - rets login different request for all agents info
                rets_agg_res_agents = get_res_and_build_dict_for_rets_mls(vendor, since_time_iso, to_time_iso,
                                                                          limit, service_flag='agents')
            if vendor in ['sfar', 'metrolist', 'baries']:  # proxy with endpoint of agents
                try:
                    total_count, dict_data = get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso,
                                                                                  to_time_iso, service_flag='agents')
                    if len(dict_data) > 0:
                        proxy_agg_res_agents = build_agg_res_agents(dict_data, proxy_agg_res_agents)
                        while int(total_count) >= offset:
                            offset += limit
                            if offset >= int(total_count):
                                break
                            count, dict_data = get_res_and_build_dict_for_proxy_mls(vendor, since_time_iso,
                                                                                    to_time_iso, service_flag='agents')
                            proxy_agg_res_agents = build_agg_res_agents(dict_data, proxy_agg_res_agents)
                        offset = 0
                except(Timeout, TooManyRedirects, RequestException):
                    print(f'Error in getting response for: {vendor}')
                    test_result_flag = False
            mls_agg_res_agent = combine_dict(mls_agg_res_agent, rest_api_agg_res_agents)
            mls_agg_res_agent = combine_dict(mls_agg_res_agent, rets_agg_res_agents)
            mls_agg_res_agent = combine_dict(mls_agg_res_agent, proxy_agg_res_agents)
        postg_agents_dict = query_common_for_agents_info()
        mis_list_in_dim_agent, plus_list_in_dim_agent = compare_mls_agg_res_vs_common_dim_agents_dicts(
            postg_agents_dict, mls_agg_res_agent)
        if len(mis_list_in_dim_agent) == 0:
            test_result_flag = True
        print(f'Miss list of mls agents vs common dim agents: {mis_list_in_dim_agent}')
        env_vars.logger.critical(f'Miss list of mls agents vs common dim agents: {len(mis_list_in_dim_agent)}')
        print(f'Plus list of mls agents vs common dim agents: {plus_list_in_dim_agent}')
        env_vars.logger.critical(f'Plus list of mls agents vs common dim agents: {len(plus_list_in_dim_agent)}')
    except (ConnectionError, ConnectionRefusedError):
        env_vars.logger.critical('Did not managed to get connection string')
        print('Did not managed to get connection string')
        test_result_flag = False
    assert test_result_flag


@pytest.mark.test_zipcode_service_area_alignment
def test_zipcode_service_area_alignment(envin, aws_new, vendor):
    res = False
    flag = True
    zip_code_list = []
    if str(aws_new).lower() != 'true':
        aws_new = False
    params.aws_new_account = aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    if vendor and vendor != 'None':
        vendors2_test = [vendor]
    else:
        vendors2_test = params.vendors2_test
    try:
        env_vars.logger = startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        get_connection_strings(envin)
        log_message = f'The Test is running on {envin}\nThe purpose of this test if to validate alignment ' \
                      f'between 2 collection and verify zipcode and area service\n'
        print(log_message)
        env_vars.logger.info(log_message)
        test_type = 'zipcode and service area alignment'
        print(f'now running test: {test_type}')
        for vendor in vendors2_test:
            vendor_identifiers = gen_queries.provider_name_to_id.get(vendor)
            db_mongo_args_listings = get_db_mongo_args(envin, 'listings')
            db_mongo_args_zip_cod_coverage = get_db_mongo_args(envin, 'zipcodesCoverage')
            full_query = query_mongo_listing_collection(vendor_identifiers)
            mongo_res_listing_dict = {}
            mongo_res_for_zip_code_coverage_dict = {}
            start = time.time()
            connect = None
            mongo_res_for_listing = mongo_class.get_aggregate_query_mongo(db_mongo_args_listings, full_query, connect)
            if len(mongo_res_for_listing) == 0:
                print(f'Error in getting response from listings collection for mls: {vendor}')
                flag = False
            end = time.time()
            print("{} - Mongo listings collection - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
            print(f"Total zipcodes in listings collection is: {len(mongo_res_for_listing)}")
            for mres in mongo_res_for_listing:
                zipcode = mres.get('zipCode')
                if zipcode not in zip_code_list:
                    zip_code_list.append(zipcode)
                in_service_area = mres.get('inServiceArea')
                if not mongo_res_listing_dict.get(zipcode):
                    mongo_res_listing_dict[zipcode] = in_service_area
            env_vars.len_of_mongo_res_listing_dict += len(mongo_res_listing_dict)
            full_query = query_mongo_zip_code_coverage_collection(zip_code_list)
            start = time.time()
            connect = None
            mongo_res_for_zip_code_coverage = mongo_class.get_aggregate_query_mongo(db_mongo_args_zip_cod_coverage, full_query, connect)
            if len(mongo_res_for_zip_code_coverage) == 0:
                print(f'Error in getting response from zipCodeCoverage collection for mls: {vendor}')
                flag = False
            end = time.time()
            print("{} - Mongo zip code coverage - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
            print(f"Total zipcodes in listings collection is: {len(mongo_res_for_zip_code_coverage)}")
            for mres in mongo_res_for_zip_code_coverage:
                zipcode = mres.get('zipCode')
                in_service_area = mres.get('inServiceArea')
                if not mongo_res_for_zip_code_coverage_dict.get(zipcode):
                    mongo_res_for_zip_code_coverage_dict[zipcode] = in_service_area
            mis_lis = check_results_class.which_not_in(not_in_here=mongo_res_listing_dict,
                                                       from_here=mongo_res_for_zip_code_coverage_dict)
            plus_lis = check_results_class.which_not_in(not_in_here=mongo_res_for_zip_code_coverage_dict,
                                                        from_here=mongo_res_listing_dict)
            if flag:
                res = print_zip_code_coverage_alignment_result(mis_lis, plus_lis, vendor)
        print_total_result_for_zip_code_coverage()
    except (ConnectionError, Exception):
        res = False
        info = sys.exc_info()
        print(info)
    assert res
