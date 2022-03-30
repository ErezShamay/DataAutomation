import datetime
import itertools
import re
import sys
import time
import traceback
from multiprocessing.pool import ThreadPool

import dateutil.parser
import psycopg2
import pytest
import pytz
import requests
from pymongo import MongoClient

from .config import params, env_vars, gen_queries
from ..gen_config import gen_params
from ..gen_features import check_results_class

check_results_class=check_results_class.check_results_class()
from ...db_tools.postgress_db import postgress_class

postgress_class=postgress_class.postgress_class()
from ...db_tools.mongo_db import mongo_class

mongo_class=mongo_class.mongo_class()

from ..gen_features.enrichment_class import enrichment

enrichment_class=enrichment()

test=None
logger=''

vendors2_test=params.vendors2_test
# delta_days=params.delta_days
# delta_hours=params.delta_hours
# delta_months=params.delta_months
mls_vendor_type=gen_params.mls_vendor_type
vendors_map=gen_params.vendors_map
vendors_filters=gen_params.vendors_filters
vendors_join_filters=gen_params.vendors_join_filters

DB_GOLD=gen_params.DB_GOLD
DB_MONGO=params.DB_MONGO
DB_WN=gen_params.DB_WN
DB_PSG_DUP=gen_params.DB_PSG_DUP
DB_PSG_BB = gen_params.DB_PSG_BB
THRSHOLD=500


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


def psg_dup_connect(env, connect=None):
    try:
        if connect is None:
            connect=psycopg2.connect(host=DB_PSG_DUP[env]['host'], port=DB_PSG_DUP[env]['port'],
                                     user=DB_PSG_DUP[env]['user'],
                                     password=DB_PSG_DUP[env]['password'], dbname=DB_PSG_DUP[env]['dbname'])
        cur=connect.cursor()
    except:
        connect=psycopg2.connect(host=DB_PSG_DUP[env]['host'], port=DB_PSG_DUP[env]['port'],
                                 user=DB_PSG_DUP[env]['user'],
                                 password=DB_PSG_DUP[env]['password'], dbname=DB_PSG_DUP[env]['dbname'])
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


def check_res(logger, vendor, postg_res, mong_res, dup_res):
    res=False
    try:
        duplicted=[]
        mongo_dist_res={}
        mis_lis=[]
        plus_lis=[]
        pres_ids={''}
        dup_lis=[]
        dup_ids=[]
        mongo_mis_info=0
        for dres in dup_res:
            dup_ids.append(dres[0])
        env_vars.dup_ids[vendor]=dup_ids
        for mres in mong_res:
            if mres.get('vendorMlsId'):
                if mres['vendorMlsId'] in mongo_dist_res:
                    duplicted.append(mres['vendorMlsId'])
                else:
                    mongo_dist_res[mres['vendorMlsId']]=mres
                    # mongo_dist_res.add(mres['vendorMlsId'])
            else:
                mongo_mis_info+=1
                if not env_vars.mongo_no_mlsVendorId.get(vendor):
                    env_vars.mongo_no_mlsVendorId[vendor]=[mres['_id']]
                else:
                    env_vars.mongo_no_mlsVendorId[vendor].append(mres['_id'])

        for pres in postg_res:
            pres_ids.add(pres[0])
            if pres[0] not in mongo_dist_res:
                if pres[0] not in dup_ids:
                    mis_lis.append(pres[0])
                else:
                    dup_lis.append(pres[0])
        for cres in mongo_dist_res:
            if cres not in pres_ids:
                plus_lis.append(cres)

        if len(postg_res) == 0:
            logger.critical(
                '***** {} - Zero non sold at WN -  wn = {}, comps = {} *****'.format(vendor, len(postg_res),
                                                                                     len(mong_res)))

        if len(postg_res) == len(mongo_dist_res) and len(postg_res) != 0:
            res=True
            logger.info(
                '{} - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}, duplicated_masked = {}'.format(
                    vendor,
                    len(postg_res),
                    len(mong_res),
                    len(mis_lis),
                    len(duplicted),
                    len(plus_lis),
                    len(dup_lis)))
            print(
                '{} - INFO - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}'.format(
                    vendor, len(postg_res), len(mong_res), len(mis_lis), len(duplicted),
                    len(plus_lis)))
        else:
            if len(mis_lis) > THRSHOLD or len(plus_lis) > THRSHOLD:
                res=False
                logger.critical(
                    '{} - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}, duplicated_masked = {}'.format(
                        vendor,
                        len(postg_res),
                        len(mong_res),
                        len(mis_lis),
                        len(duplicted),
                        len(plus_lis),
                        len(dup_lis)))
                print(
                    '{} - CRITICAL - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}, duplicated_masked = {}'.format(
                        vendor, len(postg_res),
                        len(mong_res), len(mis_lis),
                        len(duplicted), len(plus_lis), len(dup_lis)))

            else:
                res=True
                logger.warning(
                    '{} - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}, duplicated_masked = {}'.format(
                        vendor,
                        len(postg_res),
                        len(mong_res),
                        len(mis_lis),
                        len(duplicted),
                        len(plus_lis),
                        len(dup_lis)))
                print(
                    '{} - WARNING - wn = {}, comps = {}, missing = {}, duplicated = {}, comps_surplus = {}, duplicated_masked = {}'.format(
                        vendor, len(postg_res),
                        len(mong_res), len(mis_lis),
                        len(duplicted), len(plus_lis), len(dup_lis)))

        print(
            '\n****{} - Details\nduplicated = {},\nmissing = {},\ncomps_surplus_listings = {}\n****\nduplicated_masked = {}'.format(
                vendor,
                duplicted,
                mis_lis,
                plus_lis, dup_lis))

        if mongo_mis_info > 0:
            res=False
            logger.critical('{} - comps listing with no vendorMlsId = {}'.format(vendor, mongo_mis_info))
            print('{} - comps listing with no vendorMlsId = {}\n{}'.format(vendor, mongo_mis_info,
                                                                           env_vars.mongo_no_mlsVendorId[vendor]))

    except:
        res=False
        temp=sys.exc_info()
    return res


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


def get_db_mongo_args(envin, collection='listings'):
    db_mongo_args={
        'dbname': DB_MONGO['listings']['dbname'][envin],
        'collection': collection,
        'cluster': DB_MONGO['listings']['cluster'][envin],
        'user': DB_MONGO['user'],
        'password': DB_MONGO['password'],
        'auth': DB_MONGO['listings']['auth'][envin]
    }
    return db_mongo_args


def keep_valid_mongo(postg_res, mongo_res):
    plus_lis=check_results_class.which_not_in(postg_res, mongo_res)
    for plis in plus_lis:
        mongo_res.pop(plis)
    return mongo_res

def check_remarks(envin, vendor):
    try:
        res1=requests.get('http://retsgw.flexmls.com/rets2_3/Login?', headers={
            'Authorization': 'Digest username="sbr.rets.reali", realm="rets@flexmls.com", nonce="a34922aa30cc09a8f4814debc8fba51d", uri="/rets2_3/Login", algorithm="MD5", qop=auth, nc=00000001, cnonce="8nSImRmh", response="a98975849bdc8e506da3bd0dff3e3168", opaque="e740e530f881b719ac847f225d70ef26"'})
        res2=requests.get(
            'http://retsgw.flexmls.com/rets2_3/Search?SearchType=Property&Class=ResidentialProperty&Query=(ListingID="19-2930","20-734")&Format=COMPACT-DECODED&Limit=50&Count=1&QueryType=DMQL2&StandardNames=1',
            headers={
                'Authorization': 'Digest username="sbr.rets.reali", realm="rets@flexmls.com", nonce="a34922aa30cc09a8f4814debc8fba51d", uri="/rets2_3/Login", algorithm="MD5", qop=auth, nc=00000001, cnonce="8nSImRmh", response="a98975849bdc8e506da3bd0dff3e3168", opaque="e740e530f881b719ac847f225d70ef26"'})
        print('now testing {}'.format(vendor))

        import xmltodict, json
        my_json=res2.content.decode('utf8').replace("'", '"')
        o=xmltodict.parse(my_json)

        column_list=[]
        res_columns=o.get('RETS').get('COLUMNS')
        columns_list=res_columns.split('\t')
        data_list=[]
        res_data=o.get('RETS').get('DATA')
        for d in res_data:
            my_dict={}
            d_list=d.split('\t')
            for i in range(len(columns_list) - 1):
                my_dict[columns_list[i]]=d_list[i]

            data_list.append(my_dict)
    except:
        info=sys.exc_info()
        print(info)


    mls_query=gen_queries.get_query('no_private_remarks_filter', vendor)
    postg_res_no_remarks=postgress_class.get_query_psg(DB_WN, mls_query)
    print('{} - postg_res_no_remarks\n{}'.format(vendor, postg_res_no_remarks))
    wn_no_remarks=len(postg_res_no_remarks)

    print('{} no remarks:\n{}'.format(vendor,postg_res_no_remarks))





    mls_query=gen_queries.get_query('private_remarks_filter_all', vendor)
    postg_res_all=postgress_class.get_query_psg(DB_WN, mls_query)
    print('{} - postg_res_all\n{}'.format(vendor,postg_res_all))
    wn_all=len(postg_res_all)

    mls_query=gen_queries.get_query('private_remarks_filter', vendor)

    postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
    wn_has_private_remarks=len(postg_res)
    if len(postg_res) == 0:
        rec_agg = '{} - wn_has_private_remarks = 0'.format(vendor)
        res = False
        return res, rec_agg

    postg_res_dict={}
    market_name=gen_params.vendors_map.get(vendor)
    for pres in postg_res:
        listing_id=pres[0].get('property_id')
        if not postg_res_dict.get(listing_id):
            postg_res_dict[listing_id]=1
        else:
            postg_res_dict[listing_id]+=1

    bb_query_with="select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                  "where property_id in {} and pa_additional_remarks is not null and market_name ilike '%{}%') t".format(
        tuple(postg_res_dict.keys()), market_name)

    bb_res_with=postgress_class.get_query_psg(DB_PSG_BB[envin], bb_query_with)
    bb_res_with_dict={}
    for pres in bb_res_with:
        listing_id=pres[0].get('property_id')
        if not bb_res_with_dict.get(listing_id):
            bb_res_with_dict[listing_id]=1
        else:
            bb_res_with_dict[listing_id]+=1

    bb_query_without="select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                     "where property_id in {} and pa_additional_remarks is null and market_name ilike '%{}%') t".format(
        tuple(postg_res_dict.keys()), market_name)

    bb_res_without=postgress_class.get_query_psg(DB_PSG_BB[envin], bb_query_without)
    bb_res_without_dict={}
    for pres in bb_res_without:
        listing_id=pres[0].get('property_id')
        if not bb_res_without_dict.get(listing_id):
            bb_res_without_dict[listing_id]=1
        else:
            bb_res_without_dict[listing_id]+=1

    full_query={'currentListing.vendorMlsId': {'$in': list(postg_res_dict.keys())}}
    # mongo_count=mongo_class.get_count_query_mongo(db_mongo_args, full_query)

    mongo_res=mongo_class.get_find_query_mongo(db_mongo_args, full_query)
    mongo_count=len(mongo_res)
    in_mongo=[]
    for mres in mongo_res:
        in_mongo.append(mres['currentListing'].get('vendorMlsId'))

    in_mongo_in_bb_query="select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                         "where property_id in {} and pa_additional_remarks is not null and market_name ilike '%{}%') t".format(
        tuple(in_mongo), market_name)
    in_mongo_in_bb_res=postgress_class.get_query_psg(DB_PSG_BB[envin], in_mongo_in_bb_query)
    in_mongo_not_in_bb=len(in_mongo) - len(in_mongo_in_bb_res)
    in_bb_not_in_mongo={}

    rec_agg = '{} - wn all = {}, wn_has_private_remarks = {}, in_mongo = {}, in_bb ={}, NOT in_bb ={}, in_mongo_not_in_bb ={}, in_mongo_in_bb ={}'.format(
            vendor, wn_all, wn_has_private_remarks, mongo_count, len(bb_res_with_dict), len(bb_res_without_dict),
            in_mongo_not_in_bb, len(in_mongo_in_bb_res))

    print(in_mongo_in_bb_res)
    res=True
    # vendor, postg_res, mong_res = get_act_listings(logger, envin, params, vendor, test_type)

    # res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
    #                                                   dup_res)
    # print(res)
    return res, rec_agg
@pytest.mark.test_active_remarks_wn_vs_mongo
def test_active_remarks_wn_vs_mongo(envin):
    env_vars.env=envin
    start=time.time()
    try:
        res=False
        res_acc=[]

        # envin='prod'
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        env_vars.logger.info(
            'The Test is running on {0} env, comparing Active listings on Wolfnet to Active on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', THRSHOLD))
        pool=ThreadPool(processes=2)
        async_result=[]
        async_result_conc=[]
        test_type='properties'
        rec_agg=[]

        for vendor in vendors2_test:
            if vendor == 'sandicor':
                continue
            async_result.append(pool.apply_async(check_remarks, args=(envin, vendor)))

        pool.close()
        pool.join()

        #env_vars.mongo_res_photos_count=mongo_res_photos_count
        # pool.close()
        # pool.join()
        # async_result_conc.append([r.get() for r in async_result])
        # params.conn_wn.close()
        # # params.conn_mongo.close()
        # for asy_res in async_result_conc[0]:
        #     no_assets=[]
        #     vendor=asy_res[0]
        #     postg_res=asy_res[1]
        #     mong_res=asy_res[2]
        #
        #     res=check_res(env_vars.logger, vendor, postg_res, mong_res, dup_res)
        #     res_acc.append(res)
        #
        #     for pres in postg_res:
        #         if not env_vars.psg_non_sold.get(vendor):
        #             env_vars.psg_non_sold[vendor]=[pres[0]]
        #         else:
        #             env_vars.psg_non_sold[vendor].append(pres[0])
        #
        #     for mres in mong_res:
        #         if mres.get('vendorMlsId'):
        #             if mres.get('assets'):
        #
        #                 if not env_vars.mongo_non_sold.get(vendor):
        #                     env_vars.mongo_non_sold[vendor]={mres['vendorMlsId']: mres['assets']}
        #                 else:
        #                     env_vars.mongo_non_sold[vendor][mres['vendorMlsId']]=mres['assets']
        #             else:
        #                 no_assets.append(mres.get('vendorMlsId'))
        #
        #
        #         else:
        #             failure_message='{} - object_id = {} mongo results missing info. vendorMlsId = {}, photos found = yes, listingStatus = {}'.format(
        #                 vendor, mres.get('_id'), mres.get('vendorMlsId'), mres.get('listingStatus'))
        #             if not env_vars.photos_failure_info.get(vendor):
        #                 env_vars.photos_failure_info[vendor]=[failure_message]
        #             else:
        #                 env_vars.photos_failure_info[vendor].append(failure_message)
        #     if len(no_assets) > 0:
        #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
        #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))
        async_result_conc.append([r.get() for r in async_result])
        for asyn in async_result_conc[0]:
            res_acc.append(asyn[0])
            rec_agg.append(asyn[1])

        for rec in rec_agg:
            print(rec)
        if False in res_acc:
            res=False
        else:
            res=True
    except:
        res=False
        info=sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res

@pytest.mark.test_active_remarks_wn_vs_mongo_back
def test_active_remarks_wn_vs_mongo_back(envin):
    env_vars.env=envin
    start=time.time()
    try:
        res=False
        res_acc=[]

        # envin='prod'
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        env_vars.logger.info(
            'The Test is running on {0} env, comparing Active listings on Wolfnet to Active on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', THRSHOLD))
        pool=ThreadPool(processes=2)
        async_result=[]
        async_result_conc=[]
        # test_type = 'photos'
        test_type='properties'
        mongo_res_photos_count={}
        rec_agg=[]

        for vendor in vendors2_test:
            print('now testing {}'.format(vendor))
            if vendor == 'sandicor':
                continue
            mongo_res_photos_count[vendor]={}

            query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                         '$or': [
                             {
                                 '$and': [
                                     {
                                         'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']}
                                     },
                                     {
                                         'location.address1': {'$nin': ['Undisclosed Address']}
                                     }
                                 ]
                             },
                             {
                                 '$and': [
                                     {
                                         'currentListing.listingStatus': {'$in': ['PENDING']}
                                     },
                                     {'location.address1': {'$ne': 'Undisclosed Address'}}
                                 ]
                             }
                         ],
                         'currentListing.listingType': {'$nin': ['REALI']}}

            projection={"item1": 1,
                        "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                        # "item2": 1,
                        # "assets_details": "$assets",
                        "item3": 1,
                        "vendorMlsId": "$currentListing.vendorMlsId",
                        "item4": 1,
                        "realiUpdated": "$realiUpdated",
                        "item5": 1,
                        "listingStatus": "$currentListing.listingStatus"
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

            # mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query)
            # for mres in mongo_res:
            #     listing_id=mres.get('vendorMlsId')
            #     if not mongo_res_dict.get(listing_id):
            #         mongo_res_dict[listing_id]=1
            #         mongo_res_photos_count[vendor][listing_id]=mres.get('assets')
            #     else:
            #         mongo_res_dict[listing_id]+=1

            mls_query=gen_queries.get_query('private_remarks_filter_all', vendor)
            postg_res_all=postgress_class.get_query_psg(DB_WN, mls_query)
            wn_all=len(postg_res_all)

            mls_query=gen_queries.get_query('private_remarks_filter', vendor)

            postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
            wn_has_private_remarks=len(postg_res)
            if len(postg_res)==0:
                rec_agg.append(
                    '{} - wn_has_private_remarks = 0'.format(vendor))
                continue

            postg_res_dict={}
            market_name = gen_params.vendors_map.get(vendor)
            for pres in postg_res:
                listing_id=pres[0].get('property_id')
                if not postg_res_dict.get(listing_id):
                    postg_res_dict[listing_id]=1
                else:
                    postg_res_dict[listing_id]+=1

            bb_query_with="select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                          "where property_id in {} and pa_additional_remarks is not null and market_name ilike '%{}%') t".format(
                tuple(postg_res_dict.keys()), market_name)

            bb_res_with=postgress_class.get_query_psg(DB_PSG_BB[envin], bb_query_with)
            bb_res_with_dict={}
            for pres in bb_res_with:
                listing_id=pres[0].get('property_id')
                if not bb_res_with_dict.get(listing_id):
                    bb_res_with_dict[listing_id]=1
                else:
                    bb_res_with_dict[listing_id]+=1

            bb_query_without="select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                             "where property_id in {} and pa_additional_remarks is null and market_name ilike '%{}%') t".format(
                tuple(postg_res_dict.keys()), market_name)

            bb_res_without=postgress_class.get_query_psg(DB_PSG_BB[envin], bb_query_without)
            bb_res_without_dict={}
            for pres in bb_res_without:
                listing_id=pres[0].get('property_id')
                if not bb_res_without_dict.get(listing_id):
                    bb_res_without_dict[listing_id]=1
                else:
                    bb_res_without_dict[listing_id]+=1

            full_query = {'currentListing.vendorMlsId':{'$in':list(postg_res_dict.keys())}}
            #mongo_count=mongo_class.get_count_query_mongo(db_mongo_args, full_query)

            mongo_res=mongo_class.get_find_query_mongo(db_mongo_args, full_query)
            mongo_count = len(mongo_res)
            in_mongo= []
            for mres in mongo_res:
                in_mongo.append(mres['currentListing'].get('vendorMlsId'))




            in_mongo_in_bb_query = "select row_to_json(t) from(select market_name, property_id, display_address, pa_additional_remarks, listing_status from mls_properties " \
                     "where property_id in {} and pa_additional_remarks is not null and market_name ilike '%{}%') t".format(tuple(in_mongo),market_name)
            in_mongo_in_bb_res=postgress_class.get_query_psg(DB_PSG_BB[envin], in_mongo_in_bb_query)
            in_mongo_not_in_bb=len(in_mongo) - len(in_mongo_in_bb_res)
            in_bb_not_in_mongo = {}

            rec_agg.append('{} - wn all = {}, wn_has_private_remarks = {}, in_mongo = {}, in_bb ={}, NOT in_bb ={}, in_mongo_not_in_bb ={}, in_mongo_in_bb ={}'.format(vendor, wn_all, wn_has_private_remarks,mongo_count, len(bb_res_with_dict),len(bb_res_without_dict), in_mongo_not_in_bb, len(in_mongo_in_bb_res)))

            print(in_mongo_in_bb_res)
            res = True
            # vendor, postg_res, mong_res = get_act_listings(logger, envin, params, vendor, test_type)



            # res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
            #                                                   dup_res)
            # print(res)
            res_acc.append(res)
            # async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))

        #env_vars.mongo_res_photos_count=mongo_res_photos_count
        # pool.close()
        # pool.join()
        # async_result_conc.append([r.get() for r in async_result])
        # params.conn_wn.close()
        # # params.conn_mongo.close()
        # for asy_res in async_result_conc[0]:
        #     no_assets=[]
        #     vendor=asy_res[0]
        #     postg_res=asy_res[1]
        #     mong_res=asy_res[2]
        #
        #     res=check_res(env_vars.logger, vendor, postg_res, mong_res, dup_res)
        #     res_acc.append(res)
        #
        #     for pres in postg_res:
        #         if not env_vars.psg_non_sold.get(vendor):
        #             env_vars.psg_non_sold[vendor]=[pres[0]]
        #         else:
        #             env_vars.psg_non_sold[vendor].append(pres[0])
        #
        #     for mres in mong_res:
        #         if mres.get('vendorMlsId'):
        #             if mres.get('assets'):
        #
        #                 if not env_vars.mongo_non_sold.get(vendor):
        #                     env_vars.mongo_non_sold[vendor]={mres['vendorMlsId']: mres['assets']}
        #                 else:
        #                     env_vars.mongo_non_sold[vendor][mres['vendorMlsId']]=mres['assets']
        #             else:
        #                 no_assets.append(mres.get('vendorMlsId'))
        #
        #
        #         else:
        #             failure_message='{} - object_id = {} mongo results missing info. vendorMlsId = {}, photos found = yes, listingStatus = {}'.format(
        #                 vendor, mres.get('_id'), mres.get('vendorMlsId'), mres.get('listingStatus'))
        #             if not env_vars.photos_failure_info.get(vendor):
        #                 env_vars.photos_failure_info[vendor]=[failure_message]
        #             else:
        #                 env_vars.photos_failure_info[vendor].append(failure_message)
        #     if len(no_assets) > 0:
        #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
        #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))

        for rec in rec_agg:
            print(rec)
        if False in res_acc:
            res=False
        else:
            res=True
    except:
        res=False
        info=sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment
def test_open_house_wn_vs_mongo(envin):
    start=time.time()
    print('\nTest Open House - The test is eliminating verification of listings which marked as duplicated:\n'
          'SELECT property_id, mls, hash_string, is_primary FROM public.duplicate where not is_primary\n')
    try:
        res=False
        res_acc=[]

        # envin='prod'
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        logger=env_vars.logger
        test_type='open_house'
        pool=ThreadPool(processes=2)
        async_result=[]
        async_result_conc=[]

        log_info_message='The following report implying a state of fault updates/time parsing:\nMLS = Number of listings with valid Open House in MLS,\nComps = Number of listings with valid Open House in Reali,\nMissing listings = Listings with Open House on MLS but No Open House in Reali,\nSurplus listings = Listings with No Open House on MLS but Open House in Reali,\nMissing events = Open House events on MLS missing from Reali,\nSurplus events = Open House events on Reali missing from MLS, Wrong events = Open House events on MLS with Wrong record on Reali'

        logger.info(log_info_message)

        for vendor in vendors2_test:
            if vendor == 'sandicor':
                continue
            query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
                         '$or': [
                             {
                                 '$and': [
                                     {
                                         'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']}
                                     },
                                     {
                                         'location.address1': {'$nin': ['Undisclosed Address']}
                                     }
                                 ]
                             },
                             {
                                 '$and': [
                                     {
                                         'currentListing.listingStatus': {'$in': ['PENDING']}
                                     },
                                     {'location.address1': {'$ne': 'Undisclosed Address'}}
                                 ]
                             }
                         ],
                         'currentListing.listingType': {'$nin': ['REALI']},
                         'hasOpenHouse': True}

            projection={"item1": 1,
                        "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
                        "item2": 1,
                        "vendorMlsId": "$currentListing.vendorMlsId",
                        "item3": 1,
                        "realiUpdated": "$realiUpdated",
                        "item4": 1,
                        "listingStatus": "$currentListing.listingStatus",
                        "item5": 1,
                        "openHouse": "$openHouse"
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

            mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query)
            for mres in mongo_res:
                listing_id=mres.get('vendorMlsId')
                open_house_events=mres.get('openHouse')
                if not mongo_res_dict.get(listing_id):
                    mongo_res_dict[listing_id]=open_house_events
                else:
                    mongo_res_dict[listing_id]+='Duplicated_Listing'

            mls_query=gen_queries.get_query('open_house', vendor)

            postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
            postg_res_dict={}
            for pres in postg_res:
                listing_id=pres[0].get('mlnumber')
                if not postg_res_dict.get(listing_id):
                    postg_res_dict[listing_id]=[pres[0]]
                else:
                    postg_res_dict[listing_id].append(pres[0])
            # for aggregated query
            # for pres in postg_res:
            #     listing_id=pres[0].get('mlnumber')
            #     if not postg_res_dict.get(listing_id):
            #         postg_res_dict[listing_id]= pres[0].get('array_agg')
            #     else:
            #         postg_res_dict[listing_id]+=1
            # vendor, postg_res, mong_res = get_act_listings(logger, envin, params, vendor, test_type)
            dup_res={}
            res=check_results_class.check_res_openhouse_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
                                                             dup_res)
            res_acc.append(res)
            # async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))

        if False in res_acc:
            res=False
        else:
            res=True
    except:
        res=False
        info=sys.exc_info()
        print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res


@pytest.mark.test_alignment
def test_photos_active_wn_vs_mongo():
    start=time.time()
    try:
        logger=env_vars.logger
        test_type='photos'
        res=False
        res_acc=[]
        pool=ThreadPool(processes=2)
        async_result=[]
        async_result_conc=[]
        str_list=''
        samples=500
        logger.info(
            'Test is running on comps {} latest listings per mls. querying wn_postgress and comparing photos status'.format(
                samples))

        log_info_message='The following report implying a state of fault updates:\nMising = Number of listings with photos count lower then MLS photos count,\nSurplus = Number of listings with photos count greater then MLS count,\nNo Records = Number of Listings with No records at all (MLS has records),\nMLS no records = Number of Listings on MLS with No records at all (Ticket to MLS)'

        logger.info(log_info_message)
        for vendor in vendors2_test:
            if vendor == 'sandicor':
                continue

            mongo_res_photos_count=env_vars.mongo_res_photos_count.get(vendor)
            mongo_res_photos_count_sampled=dict(itertools.islice(mongo_res_photos_count.items(), samples))
            mongo_res_photos_count_ids=list(mongo_res_photos_count_sampled.keys())

            for i in range(len(mongo_res_photos_count_ids)):
                if i != samples - 1:
                    str_list+="'{}',".format(mongo_res_photos_count_ids[i])
                else:
                    str_list+="'{}'".format(mongo_res_photos_count_ids[i])
            if str_list.endswith(','):
                str_list=str_list[:-1]

            ids={vendor: str_list}
            mls_query=gen_queries.get_query('photos', vendor, ids)

            postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
            postg_res_dict={}
            for pres in postg_res:
                listing_id=pres[0].get('property_id')
                if not postg_res_dict.get(listing_id):
                    postg_res_dict[listing_id]=pres[0].get('count')

            dup_res={}
            res=check_results_class.check_res_photos_diff(logger, vendor, postg_res_dict,
                                                          mongo_res_photos_count_sampled, dup_res)
            res_acc.append(res)

        if False in res_acc:
            res=False
        else:
            res=True
    except:
        res=False
        excp=traceback.format_exc()
        print(excp)
    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res

