import requests
import json
import csv
import datetime
import itertools
import os
import re
import shutil
import sys
import time
import traceback
from multiprocessing.pool import ThreadPool
from subprocess import check_output, STDOUT

import dateutil.parser
import pandas as pd
import psycopg2
import pytest
import pytz
from pymongo import MongoClient

from .config import env_vars
from .config import params, gen_queries


from  ....src.common.gen_config import gen_params
from  ....src.common.tools.gen_features import check_results_class
from  ....src.common.tools.aws.s3_aws import s3_class
from  ....src.common.tools.aws.sqs import sqs_class
from  ....src.common.tools.db_tools.rets import rets_class
from  ....src.common.tools.gen_tools import list_and_dicts

import copy

rets_class=rets_class.rets_class()


s3_class=s3_class.s3_class()
sqs_class=sqs_class.sqs_class()

check_results_class=check_results_class.check_results_class()
from  ....src.common.tools.db_tools.postgress_db import postgress_class

postgress_class=postgress_class.postgress_class()
from  ....src.common.tools.db_tools.mongo_db import mongo_class

mongo_class=mongo_class.mongo_class()

from  ....src.common.tools.gen_features.enrichment_class import enrichment

enrichment_class=enrichment()

test=None
logger=''
DAYS_PERIOD=365

vendors2_test = params.vendors2_test
# delta_days=params.delta_days
# delta_hours=params.delta_hours
# delta_months=params.delta_months
mls_vendor_type=gen_params.mls_vendor_type
vendors_map=gen_params.vendors_map
vendors_filters=gen_params.vendors_filters

vendors_join_filters=gen_params.vendors_join_filters
vendors_join_filters_short=gen_params.vendors_join_filters_short

DB_GOLD=gen_params.DB_GOLD
DB_MONGO=gen_params.DB_MONGO
DB_WN=gen_params.DB_WN
# DB_PSG_DUP=gen_params.DB_PSG_DUP
THRSHOLD=500
START_DATE_SAMPLING='1970-01-01'
END_DATE_SAMPLING='2099-01-01'

res_dir_path='{}/tmp/pods_memory/'.format(os.path.expanduser('~'))

THRESHOLD=4000

KUBE_ENVS={
    'dev': 'dev.realatis.com',
    'prod': 'production.realatis.com',
    'qa': 'qa.realatis.com',
    'staging': 'staging.realatis.com'
}



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


def psg_connect(connect=None):
    try:
        if connect is None:
            connect=psycopg2.connect(host=DB_WN['host'], port=DB_WN['port'], user=DB_WN['user'],
                                     password=DB_WN['password'], dbname=DB_WN['dbname'])
        cur=connect.cursor()
    except:
        connect=psycopg2.connect(host=DB_WN['host'], port=DB_WN['port'], user=DB_WN['user'],
                                 password=DB_WN['password'], dbname=DB_WN['dbname'])
        cur=connect.cursor()

    return connect, cur


def mongo_connect(envin, connect=None):
    if envin in ['dev', 'qa']:
        gen_params.mongo_coll='listings'
    if connect is None:
        connection_str="mongodb://{}:{}@{}/{}?{}".format(DB_MONGO['user'], DB_MONGO['password'],
                                                         DB_MONGO['comps']['cluster'][envin],
                                                         DB_MONGO['comps']['dbname'][envin],
                                                         DB_MONGO['comps']['auth'][envin])
        client=MongoClient(connection_str)
        db=client[DB_MONGO['comps']['dbname'][envin]]
        # connect=db['comps']
        connect=db[gen_params.mongo_coll]
        # db=client[DB_MONGO['dbname']]
        # connect=db[DB_MONGO['collection']]

    return connect


def set_query_psg(vendor='wn_crmls', test_type='properties'):
    yesterday=(datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    query_test_types={
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
    query=query_test_types[test_type]
    return query


def get_time_frame(time_start, time_end):
    if time_start == '00:00' or time_end == '00:00' or not time_start or not time_end:
        time_frame=None
    else:
        time_frame=False
        t1=time_start.split(':')
        h1=t1[0]
        t2=time_end.split(':')
        h2=t2[0]
        if int(h1) < 10:
            h1=h1.replace('0', '')  # remove leading zero from hour
            time_start=h1 + ':' + t1[1]
        if int(h2) < 10:
            h2=h2.replace('0', '')  # remove leading zero from hour
            time_end=h2 + ':' + t2[1]
        if re.search('PM', time_start, flags=re.IGNORECASE) and re.search('PM', time_end, flags=re.IGNORECASE):
            time_frame=re.sub('PM', '', time_start, flags=re.IGNORECASE) + '-' + time_end
        elif re.search('AM', time_start, flags=re.IGNORECASE) and re.search('AM', time_end, flags=re.IGNORECASE):
            time_frame=re.sub('AM', '', time_start, flags=re.IGNORECASE) + '-' + time_end
        else:
            time_frame=time_start + '-' + time_end

        if not re.search('PM', time_frame, flags=re.IGNORECASE) and not re.search('AM', time_frame,
                                                                                  flags=re.IGNORECASE):
            t1=time_start.split(':')
            h1=t1[0]
            t2=time_end.split(':')
            h2=t2[0]
            if int(h1) > 12:
                start_time=str(int(h1) - 12) + ':' + t1[1] + 'PM'
            elif int(h1) < 12:
                if int(h1) == 0:
                    start_time='12' + ':' + t1[1] + 'AM'
                else:
                    start_time=str(int(h1)) + ':' + t1[1] + 'AM'
            else:
                start_time=str(int(h1)) + ':' + t1[1] + 'PM'

            if int(h2) > 12:
                end_time=str(int(h2) - 12) + ':' + t2[1] + 'PM'
            elif int(h2) < 12:
                if int(h2) == 0:
                    end_time='12' + ':' + t2[1] + 'AM'
                else:
                    end_time=str(int(h2)) + ':' + t2[1] + 'AM'
            else:
                end_time=str(int(h2)) + ':' + t2[1] + 'PM'

            if 'PM' in start_time and 'PM' in end_time:
                time_frame=start_time.replace('PM', '') + '-' + end_time
            elif 'AM' in start_time and 'AM' in end_time:
                time_frame=start_time.replace('AM', '') + '-' + end_time
            else:
                time_frame=start_time + '-' + end_time

    return time_frame


def get_expected_date_time(date_time):
    date_time=date_time.split('.')[0]
    date_time=dateutil.parser.parse(date_time)
    date_time=date_time.strftime('%Y-%m-%d %H:%M')
    get_date=date_time.split(' ')[0]
    get_time=date_time.split(' ')[1]
    return get_date, get_time


def get_date_time(expected_start, expected_end, expected_start_time=None, expected_end_time=None):
    expected_val=''
    actual_val=''

    expected_time_frame=None

    if expected_start_time and expected_start_time:
        expected_time_frame=get_time_frame(expected_start_time, expected_end_time)

    expected_start_date, expected_start_time=get_expected_date_time(expected_start)
    expected_end_date, expected_end_time=get_expected_date_time(expected_end)

    expected_end_date=expected_end.split('.')[0]
    expected_end_date=dateutil.parser.parse(expected_end_date)
    expected_end_date=expected_end_date.strftime('%Y-%m-%d-%h-%m-%s')

    if not expected_time_frame:
        expected_time_frame=get_time_frame(expected_start_time, expected_end_time)

    return expected_start_date, expected_end_date, expected_time_frame


def check_res_open_house(logger, vendor, postg_res, mong_res):
    res=False
    try:
        duplicted=[]
        psg_duplicate_info=[]
        mongo_dist_res={}
        psg_dist_res={}
        mis_lis=[]
        plus_lis=[]
        pres_ids=[]
        mongo_mis_info=0
        postg_dist_len=0
        wrong_record=[]
        psg_fault=[]

        good_rec=[]
        for mres in mong_res:  # set mongo_dist_res = a mongo id:openhouses_list dictionary
            if mres.get('vendorMlsId'):
                if mres.get('openHouse'):
                    if mres['vendorMlsId'] in mongo_dist_res:
                        duplicted.append(mres['vendorMlsId'])
                    else:
                        mongo_dist_res[mres['vendorMlsId']]=mres['openHouse']
                else:
                    mis_lis.append(mres.get('vendorMlsId'))
            else:
                mongo_mis_info+=1
                if not env_vars.mongo_no_mlsVendorId.get(vendor):
                    env_vars.mongo_no_mlsVendorId[vendor]=mres['_id']
                else:
                    env_vars.mongo_no_mlsVendorId[vendor].append(mres['_id'])

        for pres in postg_res:  # set psg_dist_res = a postgress id:openhouses_list dictionary
            if psg_dist_res.get(pres[0]):
                if pres not in psg_dist_res[pres[0]]:
                    psg_dist_res[pres[0]].append(pres)
                else:
                    psg_duplicate_info.append(pres[0])
            else:
                psg_dist_res[pres[0]]=[pres]

        for id, val in mongo_dist_res.items():  # check surplus
            if not psg_dist_res.get(id):
                plus_lis.append(id)

        for id, val in psg_dist_res.items():  # check missing and verify exists

            if id in env_vars.dup_ids[vendor]:
                # eliminate duplicated listins: listings marked as duplicated will not be considered as missing
                continue
            res_acc=[]
            postg_dist_len+=len(val)
            if not mongo_dist_res.get(id):
                mis_lis.append(id)
            else:
                if len(val) == len(mongo_dist_res.get(id)):
                    for i in range(len(mongo_dist_res.get(id))):
                        if id == '515496':
                            print(1)
                        act_date=mongo_dist_res.get(id)[i]['timestamp']
                        act_time_frame=mongo_dist_res.get(id)[i]['time']
                        act_date=datetime.datetime.fromtimestamp(act_date / 1000, tz=pytz.timezone("Etc/GMT+8"))
                        act_date=act_date.strftime('%Y-%m-%d')
                        for j in range(len(val)):
                            expected_start=val[j][1]
                            expected_end=val[j][2]
                            expected_start_time=val[j][3]
                            expected_end_time=val[j][4]

                            psg_start_date, psg_end_date, psg_time_frame=get_date_time(expected_start, expected_end,
                                                                                       expected_start_time,
                                                                                       expected_end_time)

                            if (not psg_start_date or not psg_end_date or not psg_time_frame):
                                psg_fault.append(id)
                                break

                            if act_date == psg_start_date:
                                if act_time_frame.lower() == psg_time_frame.lower():
                                    good_rec.append(id)
                                    res_acc=[]
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
            res=True
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
            res=False
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
            '\n****{} - OEPN_HOUSE Details\nwrong info records = {},\nmissing records = {},\nsurplus records = {}\nwn_fault_rec={}****\n'.format(
                vendor,
                wrong_record,
                mis_lis,
                plus_lis, psg_fault))

        if mongo_mis_info > 0:
            res=False
            logger.critical('{} - comps listing with no vendorMlsId = {}'.format(vendor, mongo_mis_info))
            print('{} - comps listing with no vendorMlsId = {}\n{}'.format(vendor, mongo_mis_info,
                                                                           env_vars.mongo_no_mlsVendorId[vendor]))

    except:
        res=False
        temp=sys.exc_info()
    return res


def check_res_photos(logger, vendor, postg_res, mongo_photos):
    try:
        mis_lis=[]
        plus_lis=[]
        mongo_surp_info=0
        mongo_mis_info=0
        psg_photos={}
        total_wn=0
        total_comps=0
        default_two=0
        default_two_lis=[]
        low_number_psg=[]
        mongo_plus_key_lis=[]
        for id in postg_res:
            if not psg_photos.get(id[0]):
                psg_photos[id[0]]=1
            else:
                psg_photos[id[0]]+=1

        for key, val in mongo_photos.items():
            if key in psg_photos:
                if (mongo_photos[key] > 0 and mongo_photos[key] == psg_photos[key]):
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

                elif (mongo_photos[key] < 3 and psg_photos[key] > 2):
                    default_two+=1
                    default_two_lis.append(key)

                elif mongo_photos[key] != psg_photos[key]:
                    if mongo_photos[key] > psg_photos[key]:
                        mongo_surp_info+=1
                        plus_lis.append(key)
                    else:
                        mongo_mis_info+=1
                        mis_lis.append(key)
                total_wn+=psg_photos[key]
                total_comps+=mongo_photos[key]

                # print('{} - {} - photos mismatch - wn = {}, comps = {}'.format(vendor,key, psg_photos[key],mongo_photos[key]))
            else:
                mongo_plus_key_lis.append(key)

        report=(
            '{} - missing = {}, surplus_photos = {}, wn_less_than_3_photos = {}, comps_default_2_photos = {}, comps_surplus_ids_not_in_wn_photos = {}'.format(
                vendor, len(mis_lis),
                len(plus_lis), len(low_number_psg), default_two,
                len(mongo_plus_key_lis)))
        if total_wn == total_comps and len(mis_lis) == 0:
            res=True
            logger.info(report)
            print(report)
        else:
            res=False
            logger.critical(report)
            print(report)

        report_detailed=(
            '\n****{} - PHOTOS Details\nmissing = {},\nsurplus_photos = {},\nwn_less_than_3_photos = {},\ncomps_default_2_photos = {},\ncomps_surplus_listings = {}****\n'.format(
                vendor, mis_lis, plus_lis, low_number_psg, default_two_lis, mongo_plus_key_lis))
        print(report_detailed)

        if env_vars.photos_failure_info.get(vendor):
            res=False
            logger.critical('{} - comps listing with photos but no vendorMlsId = {}'.format(vendor, len(
                env_vars.photos_failure_info[vendor])))
            for err in env_vars.photos_failure_info[vendor]:
                print(err)

    except:
        res=False
        temp=sys.exc_info()
        print(temp)
    return res


def date_to_epoch(date_in):
    epoch_time=None
    if date_in:
        tz=pytz.timezone('UTC')
        if ' ' in date_in:
            date_format='%Y-%m-%d %H:%M:%S'
        else:
            date_format='%Y-%m-%d'
        dt1=datetime.datetime.strptime(date_in, date_format)
        epoch_time=datetime.datetime(dt1.year, dt1.month, dt1.day, tzinfo=tz).timestamp()
        if isinstance(epoch_time, float):
            epoch_time=int(epoch_time)
    return epoch_time * 1000


def get_act_listings(logger, envin, params, vendor, test_type='properties', start_date_in=None, end_date_in=None):
    mls_res_dict={}
    try:
        dup_res_dict={}

        test_type_queries=test_type

        mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                  "where not is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))

        postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        for res in postg_res:
            property_id=res[0].get('property_id')
            dup_res_dict[property_id]=1
        env_vars.dup_res_dict[vendor]=dup_res_dict

        mongo_res_photos_count={}
        mongo_res_photos_count[vendor]={}

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     'currentListing.listingType': {'$ne': 'REALI'}
                     }

        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    # "item2": 1,
                    # "assets_details": "$assets",
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        start=time.time()
        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']

        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        end=time.time()
        print("{} - Mongo - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
                mongo_res_photos_count[vendor][listing_id]=mres.get('assets')
            else:
                mongo_res_dict[listing_id]+=1

        mls_res_dict={}
        if vendor not in ['sandicor']:
            if start_date_in and end_date_in:
                test_type_queries='properties_start_end_date'
            elif start_date_in:
                test_type_queries='properties_start_date'
            elif end_date_in:
                test_type_queries='properties_end_date'

            mls_query=gen_queries.get_query(test_type_queries, vendor, start_date_in=start_date_in,
                                            end_date_in=end_date_in)

            postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
            postg_res_dict={}
            for pres in postg_res:
                listing_id=pres[0].get('property_id')
                postg_res_dict[listing_id]=pres[0].get('city')

            mls_res_dict=postg_res_dict

        else:
            # mls_res_dict=direct_properties_res(envin)
            rets_args=DB_RETS.get(vendor)
            mls_res_dict=rets_class.direct_properties_by_status_stream(vendor, rets_args, res_type='valid_only',
                                                                       on_market=True)

        mongo_res_photos_count[vendor]=keep_valid_mongo(mls_res_dict, mongo_res_photos_count[vendor])

    except:
        info=sys.exc_info()
        print(info)
        postg_res_dict={}
        mongo_res_dict={}
        mongo_res_photos_count=0
    return vendor, mls_res_dict, mongo_res_dict, mongo_res_photos_count


def get_act_listings_back_last(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                               end_date_in=None):
    try:
        dup_res_dict={}

        test_type_queries=test_type

        mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                  "where not is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))

        postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        for res in postg_res:
            property_id=res[0].get('property_id')
            dup_res_dict[property_id]=1
        env_vars.dup_res_dict[vendor]=dup_res_dict

        mongo_res_photos_count={}
        mongo_res_photos_count[vendor]={}

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     'currentListing.listingType': {'$ne': 'REALI'}
                     }

        if start_date_in and end_date_in:
            test_type_queries='properties_start_end_date'
        elif start_date_in:
            test_type_queries='properties_start_date'
        elif end_date_in:
            test_type_queries='properties_end_date'

        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    # "item2": 1,
                    # "assets_details": "$assets",
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']

        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
                mongo_res_photos_count[vendor][listing_id]=mres.get('assets')
            else:
                mongo_res_dict[listing_id]+=1

        mls_query=gen_queries.get_query(test_type_queries, vendor, start_date_in=start_date_in, end_date_in=end_date_in)

        postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
        postg_res_dict={}
        for pres in postg_res:
            listing_id=pres[0].get('property_id')
            postg_res_dict[listing_id]=pres[0].get('city')

        mongo_res_photos_count[vendor]=keep_valid_mongo(postg_res_dict, mongo_res_photos_count[vendor])

    except:
        info=sys.exc_info()
        print(info)
        postg_res_dict={}
        mongo_res_dict={}
        mongo_res_photos_count=0
    return vendor, postg_res_dict, mongo_res_dict, mongo_res_photos_count

def get_act_listings_back(logger, envin, params, vendor, test_type='properties', start_date_in=None, end_date_in=None):
    try:
        dup_res_dict={}

        test_type_queries=test_type

        mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                  "where not is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))

        postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
        for res in postg_res:
            property_id=res[0].get('property_id')
            dup_res_dict[property_id]=1
        env_vars.dup_res_dict[vendor]=dup_res_dict

        mongo_res_photos_count={}
        mongo_res_photos_count[vendor]={}

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     'currentListing.listingType': {'$ne': 'REALI'}
                     }

        # start_date_epoch = None
        # end_date_epoch=None
        #
        # if start_date_in:
        #     start_date_epoch=date_to_epoch('1970-01-02')
        #     if isinstance(start_date_epoch,float):
        #         start_date_epoch= int(start_date_epoch)
        #
        #
        # if end_date_in:
        #     end_date_epoch=date_to_epoch('2099-01-01')
        #
        # if start_date_epoch and end_date_epoch:
        #     query_mongo['currentListing.listDate'] = {'$gte':start_date_epoch, '$lte': end_date_epoch}
        #     test_type_queries = 'properties_start_end_date'
        # elif start_date_epoch:
        #     query_mongo['currentListing.listDate'] = {'$gte': start_date_epoch}
        #     test_type_queries='properties_start_date'
        # elif end_date_epoch:
        #     query_mongo['currentListing.listDate']={'$lte': end_date_epoch}
        #     test_type_queries='properties_end_date'

        if start_date_in and end_date_in:
            test_type_queries='properties_start_end_date'
        elif start_date_in:
            test_type_queries='properties_start_date'
        elif end_date_in:
            test_type_queries='properties_end_date'

        # query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
        #              '$and': [
        #                  {'currentListing.listingStatus': {'$exists': True}},
        #                  {
        #                      'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
        #                      'location.address1': {'$ne': 'Undisclosed Address'}
        #                  }
        #              ],
        #              'currentListing.listingType': {'$ne': 'REALI'}
        #              }
        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    # "item2": 1,
                    # "assets_details": "$assets",
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']

        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
                mongo_res_photos_count[vendor][listing_id]=mres.get('assets')
            else:
                mongo_res_dict[listing_id]+=1

        mls_query=gen_queries.get_query(test_type_queries, vendor, start_date_in=start_date_in, end_date_in=end_date_in)

        postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
        postg_res_dict={}
        for pres in postg_res:
            listing_id=pres[0].get('property_id')
            postg_res_dict[listing_id]=pres[0].get('city')

        mongo_res_photos_count[vendor]=keep_valid_mongo(postg_res_dict, mongo_res_photos_count[vendor])

    except:
        info=sys.exc_info()
        print(info)
        postg_res_dict={}
        mongo_res_dict={}
        mongo_res_photos_count=0
    return vendor, postg_res_dict, mongo_res_dict, mongo_res_photos_count

def get_act_listings_is_app(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                            end_date_in=None):
    try:
        time_back=datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
        time_back_epoch=int(time_back.timestamp())
        dup_res_dict={}

        test_type_queries=test_type

        mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                  "where is_app_primary and mls = '{}') t".format(gen_params.mls_vendor_type.get(vendor))

        postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     # 'currentListing.listingType': {'$ne': 'REALI'},
                     'createdAt': {'$gte': time_back_epoch}
                     }

        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']
        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
            else:
                mongo_res_dict[listing_id]+=1

        mongo_res_dict_on=mongo_res_dict
        mongo_res_dict={}

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$in': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     # 'currentListing.listingType': {'$ne': 'REALI'},
                     'createdAt': {'$gte': time_back_epoch}
                     }

        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']
        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
            else:
                mongo_res_dict[listing_id]+=1

        mongo_res_dict_off=mongo_res_dict
        mongo_res_dict={}
        postg_res_dict={}
        for pres in postg_res:
            listing_id=pres[0].get('property_id')
            postg_res_dict[listing_id]=1
    except:
        info=sys.exc_info()
        print(info)
        postg_res_dict=mongo_res_dict_on=mongo_res_dict_off={}

    return vendor, postg_res_dict, mongo_res_dict_on, {}, mongo_res_dict_off


def get_act_listings_is_not_app(logger, envin, params, vendor, test_type='properties', start_date_in=None,
                                end_date_in=None):
    mongo_res_dict_off={}
    try:
        time_back=datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
        time_back_epoch=int(time_back.timestamp())
        dup_res_dict={}

        test_type_queries=test_type

        mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
                  "where (not is_app_primary or is_app_primary is null)  and mls = '{}') t".format(
            gen_params.mls_vendor_type.get(vendor))

        postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)

        query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                     'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     'location.address1': {'$ne': 'Undisclosed Address'},
                     # 'currentListing.listingType': {'$ne': 'REALI'},
                     'createdAt': {'$gte': time_back_epoch}
                     }
        # start_date_epoch = None
        # end_date_epoch=None
        #
        # if start_date_in:
        #     start_date_epoch=date_to_epoch('1970-01-02')
        #     if isinstance(start_date_epoch,float):
        #         start_date_epoch= int(start_date_epoch)
        #
        #
        # if end_date_in:
        #     end_date_epoch=date_to_epoch('2099-01-01')
        #
        # if start_date_epoch and end_date_epoch:
        #     query_mongo['currentListing.listDate'] = {'$gte':start_date_epoch, '$lte': end_date_epoch}
        #     test_type_queries = 'properties_start_end_date'
        # elif start_date_epoch:
        #     query_mongo['currentListing.listDate'] = {'$gte': start_date_epoch}
        #     test_type_queries='properties_start_date'
        # elif end_date_epoch:
        #     query_mongo['currentListing.listDate']={'$lte': end_date_epoch}
        #     test_type_queries='properties_end_date'

        projection={"item1": 1,
                    "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                    # "item2": 1,
                    # "assets_details": "$assets",
                    "item3": 1,
                    "vendorMlsId": "$currentListing.vendorMlsId",
                    "item4": 1,
                    "realiUpdated": "$realiUpdated",
                    "item5": 1,
                    "listingStatus": "$currentListing.listingStatus",
                    "listDate": "$currentListing.listDate"
                    }
        full_query=[
            {
                "$match": query_mongo
            },
            {"$project": projection
             },
            {
                "$sort": {"realiUpdated": -1}
            }
        ]
        db_mongo_args=get_db_mongo_args(envin)
        mongo_res_dict={}

        connect=None
        if envin == 'prod' and hasattr(gen_params,
                                       'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
            client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                               port=27017)
            db=client['realidb-production']
            connect=db['listings']
        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=1
            else:
                mongo_res_dict[listing_id]+=1

        mongo_res_dict_on=mongo_res_dict
        # mongo_res_dict = {}
        #
        # query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
        #              'currentListing.listingStatus': {'$exists': True, '$in': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
        #              'location.address1': {'$ne': 'Undisclosed Address'},
        #              'currentListing.listingType': {'$ne': 'REALI'},
        #              'createdAt': {'$gte': time_back_epoch}
        #              }
        # # start_date_epoch = None
        # # end_date_epoch=None
        # #
        # # if start_date_in:
        # #     start_date_epoch=date_to_epoch('1970-01-02')
        # #     if isinstance(start_date_epoch,float):
        # #         start_date_epoch= int(start_date_epoch)
        # #
        # #
        # # if end_date_in:
        # #     end_date_epoch=date_to_epoch('2099-01-01')
        # #
        # # if start_date_epoch and end_date_epoch:
        # #     query_mongo['currentListing.listDate'] = {'$gte':start_date_epoch, '$lte': end_date_epoch}
        # #     test_type_queries = 'properties_start_end_date'
        # # elif start_date_epoch:
        # #     query_mongo['currentListing.listDate'] = {'$gte': start_date_epoch}
        # #     test_type_queries='properties_start_date'
        # # elif end_date_epoch:
        # #     query_mongo['currentListing.listDate']={'$lte': end_date_epoch}
        # #     test_type_queries='properties_end_date'
        #
        # projection={"item1": 1,
        #             "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
        #             # "item2": 1,
        #             # "assets_details": "$assets",
        #             "item3": 1,
        #             "vendorMlsId": "$currentListing.vendorMlsId",
        #             "item4": 1,
        #             "realiUpdated": "$realiUpdated",
        #             "item5": 1,
        #             "listingStatus": "$currentListing.listingStatus",
        #             "listDate": "$currentListing.listDate"
        #             }
        # full_query=[
        #     {
        #         "$match": query_mongo
        #     },
        #     {"$project": projection
        #      },
        #     {
        #         "$sort": {"realiUpdated": -1}
        #     }
        # ]
        # db_mongo_args=get_db_mongo_args(envin)
        # mongo_res_dict={}
        #
        # if envin == 'prod' and hasattr(gen_params,'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
        #     client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
        #                        port=27017)
        #     db=client['realidb-production']
        #     connect=db['listings']
        #     mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)
        # else:
        #     mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query)
        # for mres in mongo_res:
        #     listing_id=mres.get('vendorMlsId')
        #     if not mongo_res_dict.get(listing_id):
        #         mongo_res_dict[listing_id]=1
        #     else:
        #         mongo_res_dict[listing_id]+=1
        #
        # mongo_res_dict_off=mongo_res_dict
        # mongo_res_dict={}
        postg_res_dict={}
        for pres in postg_res:
            listing_id=pres[0].get('property_id')
            postg_res_dict[listing_id]=1
    except:
        info=sys.exc_info()
        print(info)
        postg_res_dict=mongo_res_dict_on=mongo_res_dict_off={}

    return vendor, postg_res_dict, mongo_res_dict_on, {}, mongo_res_dict_off

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size=sys.getsizeof(obj)
    if seen is None:
        seen=set()
    obj_id=id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size+=sum([get_size(v, seen) for v in obj.values()])
        size+=sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size+=get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size+=sum([get_size(i, seen) for i in obj])
    return size


def get_db_mongo_args_back(envin, collection='listings'):
    db_mongo_args={
        'dbname': DB_MONGO['listings']['dbname'][envin],
        'collection': collection,
        'cluster': DB_MONGO['listings']['cluster'][envin],
        'user': DB_MONGO['user'],
        'password': DB_MONGO['password'],
        'auth': DB_MONGO['listings']['auth'][envin]
    }
    return db_mongo_args

def get_db_mongo_args(envin, collection='listings'):
    # this function will get the relevant mongo arguments from the secrets connection string

    dbname=None
    port=None
    to_db_name=DB_MONGO.split('/')
    for tdn in to_db_name:
        if '?' in tdn:
            dbname=tdn.split('?')[0]
            break

    cluster_full=DB_MONGO.split(',')[0]
    cluster=re.split(':\d+', cluster_full)[0]

    has_port=re.findall(':\d+', cluster_full)
    if len(has_port) > 0:
        port=has_port[0].split(':')[1]
        if port.isnumeric():
            port=int(port)
    db_mongo_args={
        'collection': collection,
        'cluster': cluster,
        'port': port,
        'dbname': dbname
    }

    return db_mongo_args


def keep_valid_mongo(postg_res, mongo_res):
    plus_lis=check_results_class.which_not_in(postg_res, mongo_res)
    for plis in plus_lis:
        mongo_res.pop(plis)
    return mongo_res


def summary(test_type, envin):
    if test_type and test_type in ['properties_24', 'properties_time_frame']:
        env_vars.plus_lis=[]
    res=False
    env_vars.logger.info(
        '\nTest summary: WN Vs Mongo - {} Environemnt.\nFor full report, review "test_active_wn_vs_mongo"'.format(
            envin))
    mis_alignment_ratio=0
    if env_vars.postg_res_count:
        mis_alignment_ratio=100 * (len(env_vars.mis_lis) + len(env_vars.plus_lis) + len(
            env_vars.no_assets)) / env_vars.postg_res_count
    else:
        mis_alignment_ratio=None
    if mis_alignment_ratio > 1 or len(env_vars.mis_lis) > THRSHOLD or len(env_vars.plus_lis) > THRSHOLD:
        env_vars.logger.critical(
            '\nTotal in WN = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicte ignored = {}\n'.format(
                env_vars.postg_res_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
        print(
            'Test Fail:\nTotal in WN = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicte ignored = {}'.format(
                env_vars.postg_res_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))

    else:
        res=True
        env_vars.logger.info(
            '\nTotal in WN = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicte ignored = {}'.format(
                env_vars.postg_res_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))
        print(
            'Test Pass - Total in WN = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Surplus = {},\nno_assets = {},\nmissed_cities = {},\nduplicte ignored = {}'.format(
                env_vars.postg_res_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.plus_lis),
                len(env_vars.no_assets), len(env_vars.mis_city),
                env_vars.duplicate_count))

    return res


def set_postgress_db_from_secret_jdbc(service_name, db_source, aws_secrets):
    db_secrets={}

    host=None
    port=None
    dbname=None

    con_str=aws_secrets.get(service_name).get('db').get(db_source).get('url')
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
            'user': aws_secrets.get(service_name).get('db').get(db_source).get('user'),
            'password': aws_secrets.get(service_name).get('db').get(db_source).get('password'),
            #'collection': aws_secrets.get(service_name).get('db').get(db_source).get('schema')
        }

    return db_secrets


def get_set_secrerts_back(envin):
    db_mongo={}
    db_gold={}
    db_wn={}
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
    except:
        info=sys.exc_info()
        print(info)

    return db_mongo, db_gold, db_wn


def get_set_secrerts(envin, services):
    aws_secrets={}
    try:
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf=s3_class.get_secrets_by_service(envin, service_name, params)
                # conf, secret_name, region_name=s3_class.get_secrets_by_service(envin, service_name, params)
                # if not conf:
                #     env_vars.logger.critical('Could not get configuration from secret={} at region={}'.format(secret_name,region_name))
                #     raise SystemExit()
                aws_secrets[service_name]=conf

    except:
        info=sys.exc_info()
        print(info)

    return aws_secrets

def res_2_html(pod_name):
    import pandas as pd
    import matplotlib.pyplot as plt
    import csv
    ser=[]
    with open(res_dir_path + env_vars.res_file_name, 'r') as ms:
        reader=csv.reader(ms)
        for row in reader:
            ser.append(int(', '.join(row)))
    s=pd.DataFrame({pod_name: ser, 'Threshold': [THRESHOLD] * len(ser)})

    fig, ax=plt.subplots()
    fig.patch.set_facecolor('xkcd:dark grey')
    ax.set_facecolor('xkcd:almost black')
    ax.set_title(pod_name + ' Memory Status', color='white')
    ax.set_xlabel('Build Samples', color='white')
    ax.set_ylabel('Memory(Mbit)', color='white')

    mng=plt.get_current_fig_manager()
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
    mem_size_of=None
    shell_command='kubectl config use-context {}'.format(KUBE_ENVS[envin])

    uc=check_output(shell_command, shell=True, stderr=STDOUT,
                    universal_newlines=True).rstrip('\n')

    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(uc))

    shell_command='kubectl get pods | grep {} |cut -f 1 -d " "'.format(pod_name)

    last_pod=check_output(shell_command, shell=True, stderr=STDOUT,
                          universal_newlines=True).rstrip('\n')
    if 'timeout' in last_pod:
        env_vars.logger.critical(
            'Could not get {}, pd info, got {}'.format(pod_name, last_pod))
        return mem_size_of

    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(last_pod))

    # shell_command="kubectl top pod {} | awk '{{print $3}}'| grep -E '\d+\S?i'".format(last_pod)
    shell_command="kubectl top pod {} | awk '{{print $3}}'".format(last_pod)
    mem_status=check_output(shell_command, shell=True, stderr=STDOUT,
                            universal_newlines=True).rstrip('\n')

    print('Running {}'.format(shell_command))
    print('Getting response of: {}'.format(mem_status))

    mem_size=re.finditer(r"\d+", mem_status, re.MULTILINE)

    for matchNum, match in enumerate(mem_size, start=1):
        print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum=matchNum, start=match.start(),
                                                                            end=match.end(), match=match.group()))
        mem_size_of=match.group()

    print('Getting mem_size of: {}'.format(mem_size_of))
    return mem_size_of


def check_res(mem_size_of):
    if int(mem_size_of) > THRESHOLD:
        res=False
        print('fail - Memory size is {}'.format(mem_size_of))
        env_vars.logger.critical('Memory size is {}'.format(mem_size_of))

    else:
        res=True
        print('pass - memory size is {}'.format(mem_size_of))
        env_vars.logger.info('Memory size is {}'.format(mem_size_of))

    return res


def record_memory_size(rec_mem_size_file, mem_size_of):
    if os.path.exists(rec_mem_size_file):
        with open(rec_mem_size_file, 'a') as ms:
            writer=csv.writer(ms)
            writer.writerow([mem_size_of])
    else:
        with open(rec_mem_size_file, 'w') as ms:
            writer=csv.writer(ms)
            writer.writerow([mem_size_of])
    print('saving results record to {}'.format(rec_mem_size_file))
    with open(rec_mem_size_file, 'r') as ms:
        reader=csv.reader(ms)
        for row in reader:
            print(', '.join(row))


def copy_records_for_jenkins_artifacts(rec_mem_size_file):
    shutil.copyfile(rec_mem_size_file, env_vars.res_file_name)
    shutil.copyfile(env_vars.plot_file_name, res_dir_path + env_vars.plot_file_name)
    shutil.copyfile(env_vars.plot_html_name, res_dir_path + env_vars.plot_html_name)
    print('plot file is at {}'.format(res_dir_path + env_vars.plot_html_name))


def _test_check_mem_usage(envin, pod_name):
    if not pod_name:
        PODNAME='direct-mls-transformer'
    else:
        PODNAME=pod_name

    env_vars.res_file_name='{}_mem_status.csv'.format((PODNAME))
    env_vars.plot_file_name='{}_mem_status.png'.format((PODNAME))
    env_vars.plot_html_name='{}_mem_status.html'.format((PODNAME))

    env_vars.logger=startLogger(env_vars, logPath='kube_pod_mem_usage_testLog_{}.log'.format(envin))
    env_vars.logger.info(
        'The test is running on {} env, monitoring {} pod for memory usage. Limit thershold = {} Mb'.format(envin,
                                                                                                            PODNAME,
                                                                                                            THRESHOLD))
    print(
        'The test is running on {} env, monitoring {} pod for memory usage. Limit thershold = {} Mb '.format(envin,
                                                                                                             PODNAME,
                                                                                                             THRESHOLD))

    if not os.path.exists(res_dir_path):
        os.makedirs(res_dir_path)

    mem_size_of=get_pods_mem_size(envin, pod_name)
    if not mem_size_of:
        env_vars.logger.critical(
            'Could not get memory size of {}, got {}'.format(PODNAME, mem_size_of))
        assert False

    res=check_res(mem_size_of)

    rec_mem_size_file='{}/{}'.format(res_dir_path, env_vars.res_file_name)

    record_memory_size(rec_mem_size_file, mem_size_of)

    res_2_html(pod_name)

    assert res


@pytest.mark.test_build_mock_data
def test_build_mock_data(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    global DB_MONGO,  DB_DATA_COMMON, DB_DATA_MLS_LISTINGS
    import matplotlib
    matplotlib.use('Agg')
    env_vars.env=envin
    start=time.time()
    try:
        services = ['af-common-service']

        aws_secrets = get_set_secrerts(envin,services)
        DB_DATA_COMMON=set_postgress_db_from_secret_jdbc('af-common-service', db_source='common',
                                                               aws_secrets=aws_secrets)

        res=False
        res_acc=[]
        dup_res={}

        # envin='prod'
        env_vars.logger=startLogger(logPath='DataPipes_{}.log'.format(envin))
        env_vars.logger.info(
            '')
        mls_map={
            'mlslistings': {
                            'primary_listing_id':'ListingId',
                            'listingaddress':'UnparsedAddress',
                            'status':'MlsStatus',
                            'standard_status':'StandardStatus',
                            'housetype':'PropertyType',
                            'subtype':'PropertySubType',
                            'state':'StateOrProvince',
                            'county':'CountyOrParish',
                            'street_direction':'StreetDirPrefix',
                            'street_suffix':'Streetuffix',
                            # 'street_number':'Streetnumber',
                            'listingprice':'OriginalListPrice',
                            'originating_system_name':'OriginatingSystemName',
                            'modification_timestamp':'ModificationTimestamp',
                            'status_change_timestamp':'StatusChangeTimestamp',
                            'expiration_date':'ExpirationDate',
                            'city':'City'
            },
            'crmls': {
                'primary_listing_id': 'ListingId',
                'listingaddress': 'UnparsedAddress',
                'status': 'MlsStatus',
                'standard_status': 'StandardStatus',
                'housetype': 'PropertyType',
                'subtype': 'PropertySubType',
                'state': 'StateOrProvince',
                'county': 'CountyOrParish',
                'street_direction': 'StreetDirPrefix',
                'street_suffix': 'Streetuffix',
                # 'street_number':'Streetnumber',
                'listingprice': 'OriginalListPrice',
                'originating_system_name': 'OriginatingSystemName',
                'modification_timestamp': 'ModificationTimestamp',
                'status_change_timestamp': 'StatusChangeTimestamp',
                'expiration_date': 'ExpirationDate',
                'city': 'City'
            }

        }

        env_vars.mock_by_mls={}
        originating_system_name_map = {
            'mlslistings': 'MLSL',
            'crmls': 'CRMLS'
        }
        vendors = ['mlslistings','crmls']
        for vendor in vendors:
            raw_data={}
            raw_data_ids={}
            raw_status={}
            raw_house_type={}
            fact_listings={}
            dup_fact={}
            mock_by_id={}
            expected_by_id={}

            mls_query="select row_to_json (t) from (SELECT listingaddress, listingprice, beds, baths, sqfeet, status, daysonmarket, description, mlsid, realihousestatus, datelisted, housetype, subtype, 'style', neighborhood, county, built, sqft, lotsize, foreclosure, shortsale, associationfee, associationfeefrequency, amenities, hoaname, hoaprimaryphone, petfriendly, fireplace, garage, parking, gym, cooling, pool, 'view', seniorcommunity, office, familyroom, outdoorkitchen, mediaroom, smarthome, solar, electriccarchargingstation, enlaw, laundry, flooring, parkingdescription, elementaryschool, highschool, agentfullname, agentpreferredphone, agentemail, agentstatelicense, agentkeynumeric, officekeynumeric, lockboxlocation, securitygatecode, showinginstructions, occupanttype, occupantname, occupantphone, commission, dualvariable, listservice, offerdate, closeofescrow, expirationdate, restrictions, financingterms, officename, officephone, officeemail, officeaddress, officecity, officestate, officezip, privateremarks, disclosures, inspectionreports, speciallistingconditions, parcelnumber, originalstatus, fee, streetname, streetnumber, city, internetautomatedvaluationdisplay, internetentirelistingdisplay, internetaddressdisplay, rawjson, zipcode, state, propertytype, price_preditor, photos, photos_cloudinary, longitude, latitude, accuracy, formatted_address, real_city, real_county, neighborhood_real, school_district, is_filter, filter_id, bulk_id, dw_insert_time, dw_update_time, is_duplicated, primary_listing_id, operation, hash_address_string, sourcefilename, provider_id, commisiontype, originatingsystemname, is_primary FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter and originatingsystemname='{}' limit 1000) t".format(originating_system_name_map.get(vendor))

            postg_res=postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)

            for res in postg_res:
                raw_json=res[0].get('rawjson')
                listing_id = raw_json.get(mls_map.get(vendor).get('primary_listing_id'))
                if not listing_id:
                    continue
                listing_status = raw_json.get(mls_map.get(vendor).get('status'))
                property_sub_type = raw_json.get(mls_map.get(vendor).get('subtype'))
                property_type = raw_json.get(mls_map.get(vendor).get('housetype'))

                #listing_id=raw_json.get('ListingId')
                # listing_address=raw_json.get('ListingId')
                # listing_status=raw_json.get('MlsStatus')
                # standard_status=raw_json.get('StandardStatus')
                # property_type = raw_json.get('PropertyType')
                # property_sub_type=raw_json.get('PropertySubType')
                # state=raw_json.get('StateOrProvince')
                # county = raw_json.get('CountyOrParish')
                # street_direction=raw_json.get('StreetDirPrefix')
                # street_suffix=raw_json.get('StreetSuffix')
                # street_number=raw_json.get('Streetnumber')
                # price=raw_json.get('OriginalListPrice')
                # originating_system_name=raw_json.get('OriginatingSystemName')
                # modification_timestamp = raw_json.get('ModificationTimestamp')
                # status_change_timestamp=raw_json.get('StatusChangeTimestamp')
                # expiration_date = raw_json.get('ExpirationDate')
                # city=raw_json.get('City')
                # unparsed_address=raw_json.get('UnparsedAddress')
                mapped_listing_status = res[0].get('status')
                mapped_property_type = res[0].get('housetype')
                mapped_formatted_address=res[0].get('formatted_address')
                mapped_real_city = res[0].get('real_city')
                mapped_hash_address_string = res[0].get('hash_address_string')


                if not raw_data_ids.get(listing_id):
                    raw_data_ids[listing_id]=raw_json
                else:
                    print('oops')

                if not raw_status.get(listing_status):
                    raw_status[listing_status]=[listing_id]
                else:
                    raw_status[listing_status].append(listing_id)

                if not raw_house_type.get(property_sub_type):
                    raw_house_type[property_sub_type]=[listing_id]
                else:
                    raw_house_type[property_sub_type].append(listing_id)


                if not raw_data.get(property_type):
                    raw_data[property_type] = {property_sub_type:[raw_json]}
                elif not raw_data[property_type].get(property_sub_type):
                    raw_data[property_type][property_sub_type]= [raw_json]
                else:
                    raw_data[property_type][property_sub_type].append(raw_json)

            mock_insert_list=[]
            mock_update_list=[]

            for pst, ids_list in raw_house_type.items():
                if not pst:
                    continue
                rawd_mock={}
                time_iso_now=datetime.datetime.now().isoformat(timespec='seconds') + 'Z'
                today=datetime.datetime.today()
                time_iso_today=datetime.datetime(year=today.year, month=today.month,
                                                 day=today.day, hour=0, second=0).isoformat(timespec='seconds') + 'Z'

                ModificationTimestamp_mock=time_iso_now
                StatusChangeTimestamp_mock=time_iso_now

                rawd = raw_data_ids.get(ids_list[0])
                ListingId_mock='Test' + rawd.get('ListingId')
                lis_id_mock = ListingId_mock
                if not lis_id_mock:
                    print(1)

                unparsed_address = rawd.get('UnparsedAddress')
                sn=re.findall('\d+', unparsed_address)
                if len(sn)>0:
                    street_number = str(int(sn[0])+10)
                    street_number = '0000'
                    UnparsedAddress_mock = street_number + ' ' + unparsed_address.split(' ', 1)[1]

                for key, val in rawd.items():
                    if key not in ['ListingId', 'UnparsedAddress', 'ModificationTimestamp', 'StatusChangeTimestamp']:
                        rawd_mock[key] = val
                    else:
                        rawd_mock[key] = eval('{}_mock'.format(key))

                listing_status=rawd_mock.get(mls_map.get(vendor).get('status'))
                status_sub_type=listing_status + '_' + pst
                expected_by_id[lis_id_mock] = {}
                expected_res = {}

                for map_key, raw_key in mls_map[vendor].items():
                    expected_res[map_key]=rawd_mock.get(raw_key)
                expected_by_id[lis_id_mock]={status_sub_type: expected_res}



                mock_by_id[lis_id_mock]={status_sub_type: rawd_mock}

                mock_insert_list.append(rawd_mock)

                time_iso_now=datetime.datetime.now().isoformat(timespec='seconds') + 'Z'
                rawd_update_mock= copy.deepcopy(rawd_mock)
                rawd_update_mock[mls_map.get(vendor).get('status')] = 'Closed'
                rawd_update_mock['ModificationTimestamp'] = time_iso_now
                rawd_update_mock['StatusChangeTimestamp']= time_iso_now

                mock_update_list.append(rawd_update_mock)


            mock_insert_data = {'value':mock_insert_list}
            mock_update_data={'value': mock_update_list}

            mock_insert = json.dumps(mock_insert_data)

            mock_update=json.dumps(mock_update_data)
            expected_by_id_json = json.dumps(expected_by_id)

            env_vars.mock_by_mls[vendor]={
                'mock_insert':mock_insert,
                'mock_insert_expected':expected_by_id_json,
                'mock_update': mock_update
            }

        for vendor, mocks in env_vars.mock_by_mls.items():
            for mock_type, mock_data in mocks.items():
                time_iso_now=datetime.datetime.now().isoformat(timespec='seconds') + 'Z'
                with open ('{}_{}_{}.json'.format(vendor,mock_type,time_iso_now),'w') as mock_file:
                    mock_file.write(mock_data)
    except:
        res=False
        info=sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res
