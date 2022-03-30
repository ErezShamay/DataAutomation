import urllib
import datetime
import sys
import time
import traceback
from multiprocessing.pool import ThreadPool

import dateutil.parser
import psycopg2
import pytest
import pytz
from pymongo import MongoClient

from .config import params
from  ....src.common.gen_config import gen_params
from  ....src.common.tools.gen_features import check_results_class
from  ....src.common.tools.aws.s3_aws import s3_class


s3_class=s3_class.s3_class()


check_results_class=check_results_class.check_results_class()
from  ....src.common.tools.db_tools.postgress_db import postgress_class

postgress_class=postgress_class.postgress_class()
from  ....src.common.tools.db_tools.mongo_db import mongo_class

mongo_class=mongo_class.mongo_class()

from  ....src.common.tools.gen_features.enrichment_class import enrichment

from  ....src.common.tools.gen_features import get_secrets_class

from  ....src.common.gen_config.redfin_mongo_mapping import get_redfin_mongo_region_type_name
get_secrets = get_secrets_class.get_secrets_class()

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
vendors_join_filters_short=gen_params.vendors_join_filters_short

DB_GOLD=gen_params.DB_GOLD
DB_MONGO=gen_params.DB_MONGO
DB_WN=gen_params.DB_WN
DB_PSG_DUP=gen_params.DB_PSG_DUP
THRSHOLD=0


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
        'dbname': dbname,
        'connection_string': DB_MONGO
    }
    # print(DB_MONGO)
    return db_mongo_args
# def get_act_listings_redfin_first(logger, envin, params, zipcode=None, test_type='properties'):
#     dup_res_dict={}
#
#     redfin_res_by_sorce_dict={}
#
#     mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
#               "where not is_app_primary) t"
#
#     # house_filter = HouseFilter(
#     #     property_type_list=[PropertyTypeEnum.HOUSE],
#     #     max_price=PriceEnum.PRICE_10M
#     # )
#     house_filter=HouseFilter(
#         max_price=PriceEnum.PRICE_10M
#     )
#     zipcode=91331  # reachest 94027#90650	#90011
#
#     # zipcode=int(zipcode)
#     mongo_res_photos_count={}
#     mongo_res_photos_count[zipcode]={}
#     redfin_res_dict={}
#
#     houses_csv=get_property_by_zip_code(zipcode, filter=house_filter)
#     df_houses=convert_csv_to_dataframe(houses_csv)
#     with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False, 'max_colwidth', -1):
#         print(df_houses)
#     df_houses_dict=df_houses.to_dict('records')
#
#     for lis in df_houses_dict:
#         listing_id=lis.get('MLS#')
#         source=lis.get('SOURCE')
#         redfin_res_dict[listing_id]=lis.get('CITY')
#         if not redfin_res_by_sorce_dict.get(source):
#             redfin_res_by_sorce_dict[source]=[listing_id]
#         else:
#             redfin_res_by_sorce_dict[source].append(listing_id)
#
#     # postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
#     # for res in postg_res:
#     #     property_id=res[0].get('property_id')
#     #     dup_res_dict[property_id]=1
#     # env_vars.dup_res_dict[vendor]=dup_res_dict
#     #
#
#     # query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
#     #              '$or': [
#     #                  {
#     #                      '$and': [
#     #                          {
#     #                              'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']}
#     #                          },
#     #                          {
#     #                              'location.address1': {'$nin': ['Undisclosed Address']}
#     #                          }
#     #                      ]
#     #                  },
#     #                  {
#     #                      '$and': [
#     #                          {
#     #                              'currentListing.listingStatus': {'$in': ['PENDING']}
#     #                          },
#     #                          {'location.address1': {'$ne': 'Undisclosed Address'}}
#     #                      ]
#     #                  }
#     #              ],
#     #              'currentListing.listingType': {'$nin': ['REALI']}}
#     query_mongo={'location.zipCode': '{}'.format(zipcode),
#                  'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
#                  'location.address1': {'$ne': 'Undisclosed Address'},
#                  'currentListing.listingType': {'$ne': 'REALI'}
#                  }
#     projection={"item1": 1,
#                 "assets": {"$cond": {"if": {"$isArray": "$assets"}, "then": {"$size": "$assets"}, "else": 0}},
#                 # "item2": 1,
#                 # "assets_details": "$assets",
#                 "item3": 1,
#                 "vendorMlsId": "$currentListing.vendorMlsId",
#                 "item4": 1,
#                 "realiUpdated": "$realiUpdated",
#                 "item5": 1,
#                 "listingStatus": "$currentListing.listingStatus"
#                 }
#     full_query=[
#         {
#             "$match": query_mongo
#         },
#         {"$project": projection
#          },
#         {
#             "$sort": {"realiUpdated": -1}
#         }
#     ]
#     db_mongo_args=get_db_mongo_args(envin)
#     mongo_res_dict={}
#
#     if envin == 'prod' and hasattr(gen_params,'vendors_on_real_prod') and vendor not in gen_params.vendors_on_real_prod:
#         client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True, port=27017)
#         db=client['realidb-production']
#         connect=db['listings']
#         mongo_res=mongo_class.get_aggregate_query_mongo_prod(connect, full_query)
#     else:
#         mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query)
#     for mres in mongo_res:
#         listing_id=mres.get('vendorMlsId')
#         if listing_id == 'BB20067036':
#             print(1)
#         if not mongo_res_dict.get(listing_id):
#             mongo_res_dict[listing_id]=1
#             mongo_res_photos_count[zipcode][listing_id]=mres.get('assets')
#         else:
#             mongo_res_dict[listing_id]+=1
#
#     # mls_query=gen_queries.get_query(test_type, vendor)
#     #
#     # postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
#     # postg_res_dict={}
#     # for pres in postg_res:
#     #     listing_id=pres[0].get('property_id')
#     #     postg_res_dict[listing_id]=pres[0].get('city')
#     #     # if not postg_res_dict.get(listing_id):
#     #     #     postg_res_dict[listing_id]=pres[0].get('city')
#     #     # else:
#     #     #     postg_res_dict[listing_id]+=1
#
#     # mongo_res_photos_count[zipcode]=keep_valid_mongo(postg_res_dict, mongo_res_photos_count[zipcode])
#
#     # query_res1=query_res2=None
#     #
#     # if test_type in ['pa__count', 'properties_count']:
#     #     query_res1, vendor=get_query_psg(logger, params, vendor, test_type='properties_count')
#     #     query_res2, vendor=get_query_psg(logger, params, vendor, test_type='pa_count')
#     # else:
#     #     query_res1, vendor=get_query_psg(logger, params, vendor, test_type=test_type)
#     #     query_res2=get_query_mongo(logger, envin, params, vendor, query_res1, test_type=test_type)
#
#     return zipcode, redfin_res_dict, mongo_res_dict, mongo_res_photos_count

def get_act_mongo(envin, region_type, region_name=None, region_mongo_id=None, listing_ids=None):


    if not listing_ids:
        listing_ids = []
    mongo_res_photos_count = {}
    mongo_res_dict = {}
    try:

        query_mongo_by_region_type = {
            'county': {'location.county': '{}'.format(region_name),
                     'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     #'location.address1': {'$ne': 'Undisclosed Address'},
                    'toIndex':True
                     # 'currentListing.listingType': {'$ne': 'REALI'}
                       },
            'zip_code': {'location.zipCode': '{}'.format(region_mongo_id),
                     'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     #'location.address1': {'$ne': 'Undisclosed Address'},
                    'toIndex': True
                     # 'currentListing.listingType': {'$ne': 'REALI'}
                     },
            'city': {'location.areaId': region_mongo_id,
                     'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                     #'location.address1': {'$ne': 'Undisclosed Address'},
                     'toIndex': True
                     },
           'listing_ids': {"currentListing.vendorMlsId":{"$in": listing_ids}
                          # 'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                          # 'location.address1': {'$ne': 'Undisclosed Address'}
                                      }
                     }


        # query_mongo={'location.zipCode': '{}'.format(zipcode),
        #              'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
        #              'location.address1': {'$ne': 'Undisclosed Address'},
        #              'currentListing.listingType': {'$ne': 'REALI'}
        #              }

        # query_mongo={'location.areaId': region_mongo_id,
        #              'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
        #              'location.address1': {'$ne': 'Undisclosed Address'}
        #              }

        query_mongo = query_mongo_by_region_type.get(region_type)
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
                    "item6": 1,
                    "address": "$location.address1",
                    "item7": 1,
                    "mlsVendorType": "$currentListing.mlsVendorType"
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


        connect=None

        mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

        for mres in mongo_res:
            listing_id=mres.get('vendorMlsId')
            if not mongo_res_dict.get(listing_id):
                mongo_res_dict[listing_id]=[mres]
                mongo_res_photos_count[listing_id]=mres.get('assets')
            else:
                mongo_res_dict[listing_id].append(mres)
    except:
        info = sys.exc_info()
        print(info)

    return mongo_res_dict, mongo_res_photos_count



def get_act_redfin(region_name, region_type, redfin_res_full_dict):

    redfin_res_by_source_dict={}
    redfin_res_dict={}

    # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
    #           "where not is_app_primary) t"

    # house_filter = HouseFilter(
    #     property_type_list=[PropertyTypeEnum.HOUSE],
    #     max_price=PriceEnum.PRICE_10M
    # )
    # house_filter=HouseFilter(
    #     max_price=PriceEnum.PRICE_10M
    # )
    # zipcode= 91331# reachest 94027#90650	#90011
    #
    # #zipcode=int(zipcode)
    # mongo_res_photos_count={}
    # mongo_res_photos_count[zipcode]={}
    # redfin_res_dict={}
    #
    # houses_csv=get_property_by_zip_code(zipcode, filter=house_filter)
    # df_houses=convert_csv_to_dataframe(houses_csv)
    # with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False, 'max_colwidth', -1):
    #     print(df_houses)
    # df_houses_dict=df_houses.to_dict('records')

    for lis in redfin_res_full_dict:

        property_type=lis.get('PROPERTY TYPE')
        if property_type and property_type.lower() in['land', 'other', 'vacant land', 'farm']:
            continue
        region_name_redfin = 'NA'
        days_on_market = 'NA'
        listing_id = lis.get('MLS#')
        source = lis.get('SOURCE')
        days_on_market = lis.get('DAYS ON MARKET')
        city=lis.get('CITY')
        address = lis.get('ADDRESS')
        zip_code = lis.get('ZIP OR POSTAL CODE')


        if region_type == 'city':
            region_name_redfin = city
        elif region_type == 'zip_code':
            region_name_redfin = zip_code
            if not region_name_redfin or str(region_name_redfin)!=str(region_name):#Redfin Algorithm might include additional region names eater then the one searched
                continue
        else:
            region_name_redfin = region_name


        # if source == 'CRMLS':
        #     if property_type not in ['Residential', 'Residential Income', 'Single Family Residential', 'Townhouse',
        #                              'Condo/Co-op', 'Multi-Family (2-4 Unit)', 'Mobile/Manufactured Home']:
        #         continue
        redfin_res_dict[listing_id]= {'region_name_redfin':region_name_redfin,'days_on_market':days_on_market, 'city':city,'address':address, 'zip_code':zip_code, 'property_type':property_type, 'source':source}
        if not redfin_res_by_source_dict.get(source):
            redfin_res_by_source_dict[source]=[listing_id]
        else:
            redfin_res_by_source_dict[source].append(listing_id)

    return redfin_res_dict, redfin_res_by_source_dict

def get_act_listings_redfin(logger, envin, params, region_name, redfin_res_full_dict, region_mongo_id=None):
    dup_res_dict={}

    redfin_res_by_source_dict={}
    redfin_res_dict={}
    mongo_res_photos_count={}
    mongo_res_photos_count[region_name]={}

    # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
    #           "where not is_app_primary) t"

    # house_filter = HouseFilter(
    #     property_type_list=[PropertyTypeEnum.HOUSE],
    #     max_price=PriceEnum.PRICE_10M
    # )
    # house_filter=HouseFilter(
    #     max_price=PriceEnum.PRICE_10M
    # )
    # zipcode= 91331# reachest 94027#90650	#90011
    #
    # #zipcode=int(zipcode)
    # mongo_res_photos_count={}
    # mongo_res_photos_count[zipcode]={}
    # redfin_res_dict={}
    #
    # houses_csv=get_property_by_zip_code(zipcode, filter=house_filter)
    # df_houses=convert_csv_to_dataframe(houses_csv)
    # with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False, 'max_colwidth', -1):
    #     print(df_houses)
    # df_houses_dict=df_houses.to_dict('records')

    for lis in redfin_res_full_dict:
        listing_id=lis.get('MLS#')
        source=lis.get('SOURCE')
        property_type=lis.get('PROPERTY TYPE')
        if source == 'CRMLS':
            if property_type not in ['Residential', 'Residential Income', 'Single Family Residential', 'Townhouse',
                                     'Condo/Co-op', 'Multi-Family (2-4 Unit)', 'Mobile/Manufactured Home']:
                continue
        redfin_res_dict[listing_id]=lis.get('CITY')
        if not redfin_res_by_source_dict.get(source):
            redfin_res_by_source_dict[source]=[listing_id]
        else:
            redfin_res_by_source_dict[source].append(listing_id)

    # postg_res=postgress_class.get_query_psg(DB_GOLD[envin], mls_query)
    # for res in postg_res:
    #     property_id=res[0].get('property_id')
    #     dup_res_dict[property_id]=1
    # env_vars.dup_res_dict[vendor]=dup_res_dict
    #

    # query_mongo={'currentListing.mlsVendorType': mls_vendor_type[vendor],
    #              '$or': [
    #                  {
    #                      '$and': [
    #                          {
    #                              'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']}
    #                          },
    #                          {
    #                              'location.address1': {'$nin': ['Undisclosed Address']}
    #                          }
    #                      ]
    #                  },
    #                  {
    #                      '$and': [
    #                          {
    #                              'currentListing.listingStatus': {'$in': ['PENDING']}
    #                          },
    #                          {'location.address1': {'$ne': 'Undisclosed Address'}}
    #                      ]
    #                  }
    #              ],
    #              'currentListing.listingType': {'$nin': ['REALI']}}
    query_mongo={'location.county': '{}'.format(region_name),
                 'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                 'location.address1': {'$ne': 'Undisclosed Address'},
                 'currentListing.listingType': {'$ne': 'REALI'}
                 }

    # query_mongo={'location.zipCode': '{}'.format(zipcode),
    #              'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
    #              'location.address1': {'$ne': 'Undisclosed Address'},
    #              'currentListing.listingType': {'$ne': 'REALI'}
    #              }

    query_mongo={'location.areaId': region_mongo_id,
                 'currentListing.listingStatus': {'$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
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
                "mlsVendorType": "$currentListing.mlsVendorType"
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
    # if envin == 'prod' and hasattr(gen_params,
    #                                'vendors_on_real_prod') and env_vars.vendor not in gen_params.vendors_on_real_prod:
    #     client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
    #                        port=27017)
    #     db=client['realidb-production']
    #     connect=db['listings']
    mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

    for mres in mongo_res:
        listing_id=mres.get('vendorMlsId')
        if not mongo_res_dict.get(listing_id):
            mongo_res_dict[listing_id]=1
            mongo_res_photos_count[region_name][listing_id]=mres.get('assets')
        else:
            mongo_res_dict[listing_id]+=1

    # mls_query=gen_queries.get_query(test_type, vendor)
    #
    # postg_res=postgress_class.get_query_psg(DB_WN, mls_query)
    # postg_res_dict={}
    # for pres in postg_res:
    #     listing_id=pres[0].get('property_id')
    #     postg_res_dict[listing_id]=pres[0].get('city')
    #     # if not postg_res_dict.get(listing_id):
    #     #     postg_res_dict[listing_id]=pres[0].get('city')
    #     # else:
    #     #     postg_res_dict[listing_id]+=1

    # mongo_res_photos_count[zipcode]=keep_valid_mongo(postg_res_dict, mongo_res_photos_count[zipcode])

    # query_res1=query_res2=None
    #
    # if test_type in ['pa__count', 'properties_count']:
    #     query_res1, vendor=get_query_psg(logger, params, vendor, test_type='properties_count')
    #     query_res2, vendor=get_query_psg(logger, params, vendor, test_type='pa_count')
    # else:
    #     query_res1, vendor=get_query_psg(logger, params, vendor, test_type=test_type)
    #     query_res2=get_query_mongo(logger, envin, params, vendor, query_res1, test_type=test_type)

    return region_name, redfin_res_dict, redfin_res_by_source_dict, mongo_res_dict, mongo_res_photos_count

def get_act_fact_valid(db_args):
    postg_res_dict={}
    dup_res_dict={}
    # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
    #           "where not is_app_primary) t"

    mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter) t"

    # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
    postg_res=postgress_class.get_query_psg(db_args, mls_query)

    for res in postg_res:
        mls_id=res[0].get('mlsid')
        postg_res_dict[mls_id]=res[0].get('city')
    return postg_res_dict

def get_act_fact_all(db_args):
    postg_res_dict={}

    mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter) t"

    mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings) t"

    # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
    postg_res=postgress_class.get_query_psg(db_args, mls_query)

    for res in postg_res:
        mls_id=res[0].get('mlsid')
        postg_res_dict[mls_id]={'city':res[0].get('city'), 'status':res[0].get('status'), 'is_duplicated':res[0].get('is_duplicated'), 'is_filter': res[0].get('is_filter')}

    return postg_res_dict


def get_act_fact_listings(db_args, listings_list):

    listings = str(listings_list).replace('[', '').replace(']', '')
    postg_res_dict={}

    mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where mlsid in({})) t".format(listings)

    # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
    postg_res=postgress_class.get_query_psg(db_args, mls_query)

    for res in postg_res:
        mls_id=res[0].get('mlsid')
        postg_res_dict[mls_id] = {
            'status': res[0].get('status'),
            'realihousestatus': res[0].get('realihousestatus'),
            'originalstatus':res[0].get('originalstatus'),
            'is_duplicated':res[0].get('is_duplicated'),
            'primary_listing_id': res[0].get('primary_listing_id'),
            'is_filter': res[0].get('is_filter'),
            'filter_id': res[0].get('filter_id'),
            'originatingsystemname': res[0].get('originatingsystemname'),
            'housetype': res[0].get('housetype'),
            'propertytype': res[0].get('propertytype'),
            'subtype': res[0].get('subtype'),
            'comp_proxy_status':res[0].get('comp_proxy_status'),
            'listingaddress':res[0].get('listingaddress'),
            'city': res[0].get('city'),
            'real_city': res[0].get('real_city'),
            'county': res[0].get('county'),
            'real_county': res[0].get('real_county'),
            'zipcode': res[0].get('zipcode'),
        }

    return postg_res_dict


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


def keep_valid_mongo(postg_res, mongo_res):
    plus_lis=check_results_class.which_not_in(postg_res, mongo_res)
    for plis in plus_lis:
        mongo_res.pop(plis)
    return mongo_res


def summary(test_type, envin):
    if test_type and test_type == 'properties_24':
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


# import kubernetes
# from kubernetes import client, config, watch
import csv
import os
import re
from subprocess import check_output, STDOUT

from .config import env_vars

res_dir_path='{}/tmp/pods_memory/'.format(os.path.expanduser('~'))

THRESHOLD=4000

KUBE_ENVS={
    'dev': 'dev.realatis.com',
    'prod': 'production.realatis.com',
    'qa': 'qa.realatis.com',
    'staging': 'staging.realatis.com'
}


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


import pandas as pd
import io


# from redfin_houses.house_filter import HouseFilter, PriceEnum
# from redfin_houses.redfin import query_house_list


def convert_csv_to_dataframe(csv):
    f=io.StringIO(csv)
    df=pd.read_csv(f)
    return df


# def get_property_by_zip_code(zipcode, filter=None):
#     print("Searching within zipcode: {}".format(zipcode))
#     print("Applying filter: {}".format(filter.to_query_str()))
#     response=query_house_list("zipcode/{}".format(zipcode), filter)
#     return response

def set_postgress_db_from_secret_jdbc(service_name, db_source, aws_secrets):
    db_secrets={}

    host=None
    port=None
    dbname=None

    try:
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
    except:
        print('Failed to retrieve secrets for {} - Please verify Secret config exists'.format(service_name))

    return db_secrets



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

# def get_redfin_mongo_region_type_name(logger,region_type, region_name):
#
#     region_type_name_in = 'NA'
#     region_name_id ='NA'
#     region_type_id = 'NA'
#     region_mongo_id ='NA'
#
#
#     try:
#         region_name_id=None
#         region_mongo_id=None
#         region_type_id=None
#         region_type_name_in = '{}:{}'.format(region_type,region_name)
#
#         redfin_zip_code_to_region_id={
#             '90025': '37472',
#             '90011': '37458',
#             '94002': '38960',
#             '94005':'38961',
#             '94010':'38962',
#             '94011': '38963',
#             '94014': '38965',
#             '94022': '38973',
#             '94023': '38974',
#             '94024': '38975',
#             '94040': '38985',
#             '94041': '38986',
#             '94042': '38987',
#             '95003': '39354',
#             '95005': '39356',
#             '95006': '39357',
#             '95010': '39361',
#             '95017': '39367',
#             '95076': '39409',
#             '93426': '38715',
#             '93451': '38738',
#             '93905': '38930',
#             '95004': '38932',
#             '95039': '39383',
#             '95012': '39363',
#             '93901': '38930',
#             '93210': '38612',
#             '93930': '38947',
#             '95023': '39372',
#             '95043': '39386',
#             '95045': '39388',
#             '95075': '39408',
#             '90620': '37643',
#             '90621': '37644',
#             '90623': '37646',
#             '90630': '37648',
#             '90631': '37649',
#             '91901': '37997',
#             '91902': '37998',
#             '91906': '38001',
#             '91910': '38004',
#             '92132': '38131',
#             '90001': '37448',
#             '90002': '37449',
#             '90003': '37450',
#             '90004': '37451',
#             '90005': '37452',
#             "91007": "37452",
#             "91008": "37730",
#             "91010": "37732",
#             "91016": "37735",
#             "91024": "37740",
#             "91030": "37742",
#             "91101": "37751",
#             "91103": "37753",
#             "91104": "37754",
#             "91105": "37755",
#             "91106": "37756",
#             "91107": "37757",
#             "91108": "37758",
#             "91702": "37923",
#             "91706": "37924",
#             "91722": "37932",
#             "91723": "37933",
#             "91724": "37934",
#             "91731": "37937",
#             "91732": "37938",
#             "91733": "37939",
#             "91740": "37944",
#             "91741": "37945",
#             "91744": "37947",
#             "91745": "37948",
#             "91746": "37949",
#             "91748": "37951",
#             "91750": "37953",
#             "91754": "37955",
#             "91755": "37956",
#             "91763": "37962",
#             "91765": "37964",
#             "91766": "37965",
#             "91768": "37967",
#             "91770": "37969",
#             "91773": "37972",
#             "91775": "37973",
#             "91776": "37974",
#             "91780": "37976",
#             "91789": "37981",
#             "90061": "37508",
#             "90247": "37567",
#             "90248": "37568",
#             "90249": "37569",
#             "90630": "37648",
#             "90650": "37656",
#             "90701": "37666",
#             "90703": "37668",
#             "90706": "37670",
#             "90712": "37674"
#
#         }
#
#
#         redfin_zip_code_to_region_id_by_mls={ # In order to find the Redfin region_id, Go to region_id - in the preview, search for the zipcode, click inside and go a bit to the left
#             '90025': '37472',
#             '90011': '37458',
#             'mlslistings':
#                 {
#                     'san_mateo':
#                         {
#                 '94002': '38960',
#                 '94005':'38961',
#                 '94010':'38962',
#                 '94011': '38963',
#                 '94014': '38965'
#                 },
#                     'santa_clara':
#                         {
#                     '94022':'38973',
#                     '94023':'38974',
#                     '94024': '38975',
#                     '94040':'38985',
#                     '94041':'38986',
#                     '94042': '38987'
#                     },
#                     'santa_cruz':
#                         {
#                     '95003':'39354',
#                     '95005':'39356',
#                     '95006':'39357',
#                     '95010':'39361',
#                     '95017': '39367',
#                     '95076':'39409'
#                 },
#                     'monterey':
#                         {
#                     '93426': '38715',
#                     '93451': '38738',
#                     '93905': '38930',
#                     '95004': '38932',
#                     '95039': '39383',
#                     '95012': '39363',
#                     '93901':'38930'
#                     },
#                     'San Benito':
#                         {
#                        '93210':'38612',
#                         '93930':'38947',
#                         '95023':'39372',
#                         '95043':'39386',
#                         '95045':'39388',
#                         '95075':'39408'
#
#
#                     }
#                 },
#             'crmls':
#                 {
#                     'Orange':
#                         {
#                             '90620':'37643',
#                             '90621':'37644',
#                             '90623':'37646',
#                             '90630':'37648',
#                             '90631':'37649'
#                         },
#                     'San Diego':
#                         {
#                             '91901':'37997',
#                             '91902':'37998',
#                             '91906':'38001',
#                             '91910':'38004',
#                             '92132': '38131'
#                         },
#                     'Los Angeles':
#                         {
#                             '90001': '37448',
#                             '90002': '37449',
#                             '90003': '37450',
#                             '90004': '37451',
#                             '90005': '37452'
#                         },
#                     'SCA Agents':
#                         {
#                             "91007":"37452",
#                             "91008":"37730",
#                             "91010":"37732",
#                             "91016":"37735",
#                             "91024":"37740",
#                             "91030":"37742",
#                             "91101":"37751",
#                             "91103":"37753",
#                             "91104":"37754",
#                             "91105":"37755",
#                             "91106":"37756",
#                             "91107":"37757",
#                             "91108":"37758",
#                             "91702":"37923",
#                             "91706":"37924",
#                             "91722":"37932",
#                             "91723":"37933",
#                             "91724":"37934",
#                             "91731":"37937",
#                             "91732":"37938",
#                             "91733":"37939",
#                             "91740":"37944",
#                             "91741":"37945",
#                             "91744":"37947",
#                             "91745":"37948",
#                             "91746":"37949",
#                             "91748": "37951",
#                             "91750":"37953",
#                             "91754": "37955",
#                             "91755":"37956",
#                             "91763": "37962",
#                             "91765":"37964",
#                             "91766": "37965",
#                             "91768":"37967",
#                             "91770": "37969",
#                             "91773":"37972",
#                             "91775": "37973",
#                             "91776":"37974",
#                             "91780": "37976",
#                             "91789":"37981",
#                             "90061": "37508",
#                             "90247":"37567",
#                             "90248": "37568",
#                             "90249":"37569",
#                             "90630": "37648",
#                             "90650":"37656",
#                             "90701": "37666",
#                             "90703":"37668",
#                             "90706": "37670",
#                             "90712":"37674"
#         }
#
#                 }
#         }
#
#         redfin_region_type_id={
#             'county': '5',
#             'city': '6',
#             'zip_code': '2'
#         }
#         redfin_city_to_region_id={
#             'los_angels': '11203',
#             'san_diego': '16904',
#             'san_francisco': '17151',
#             'san_mateo': '17490',
#             'santa_clara': '17675',
#             'santa_cruz':'17680',
#             'monterey':'12514',
#             'san_benito':'na'
#         }
#         redfin_status_to_id={
#             'Active_Pending': '131',
#             'Active_Comming_Soon': '9',
#             'Sold': ''
#         }
#
#         redfin_county_to_region_id={
#             'Los Angeles County': '321',
#             'San Diego County': '339',
#             'San Francisco County': '340',
#             'San Benito County': '337',
#             'San Mateo County': '343',
#             'Santa Clara County': '345',
#             'Santa Cruz County': '346',
#             'Monterey County':'329'
#         }
#
#         county_in_to_county={
#             'los_angeles_county': 'Los Angeles County',
#             'san_diego_county': 'San Diego County',
#             'san_francisco_county': 'San Francisco County',
#             'san_benito_county': 'San Benito County',
#             'san_mateo_county': 'San Mateo County',
#             'santa_clara_County': 'Santa Clara County',
#             'santa_cruz_County': 'Santa Cruz_ County',
#             'monterey_county': 'Monterey County'
#
#             #'Orange County', 'San Diego', 'Los Angeles', 'San Bernadino', 'Riverside', 'Ventura', 'San Luis','Obispo'
#         }
#
#         mongo_city_to_id={
#             'san_mateo': 76,
#             'santa_clara': 80,
#             'santa_cruz': 175,
#             'monterey': 180,
#             'san_benito': 4215
#         }
#
#         if region_type_name_in and ':' in region_type_name_in:
#             region_type=region_type_name_in.split(':')[0]
#             region_name_in=region_type_name_in.split(':')[1]
#             if region_type == 'county':
#                 region_name=county_in_to_county.get(region_name_in)
#                 region_name_id=redfin_county_to_region_id.get(region_name)
#                 region_mongo_id = county_in_to_county.get(region_name)
#             elif region_type == 'city':
#                 region_name=redfin_city_to_region_id.get(region_name_in)
#                 region_name_id=region_name
#                 region_mongo_id=mongo_city_to_id.get(region_name_in)
#             elif region_type == 'zip_code':
#                 region_name_id=redfin_zip_code_to_region_id.get(region_name_in)
#                 region_mongo_id=region_name
#
#
#         if region_type and region_name:
#             region_type_id=redfin_region_type_id.get(region_type)
#             # region_name_id=county_to_region_id.get(region_name)
#         if not redfin_region_type_id or not region_name_id:
#             print(
#                 'Test was not executed - No valid region_type_name provided = {}'.format(region_type_name_in))
#             logger.critical(
#                 'Test was not executed - No valid region_type_name provided = {}'.format(region_type_name_in))
#             assert False
#     except:
#         info = sys.exc_info()
#         print(info)
#
#
#
#     return region_type_name_in, region_type, region_name_id, region_type_id, region_mongo_id, region_name


#def get_redifin_by(logger, envin, region_type_name_in):
def get_redfin_by(region_name_id, region_type_id):

    redfin_res_full_dict={}
    try:
        # region_type=None
        # region_name=None
        # region_name_id=None
        # region_mongo_id=None

        _REQUEST_HEADER={
            'User-Agent':
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64',
            'Accept':
                'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset':
                'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Language':
                'en-US,en;q=0.8',
            'Connection':
                'keep-alive'
        }


        url='https://www.redfin.com/stingray/api/gis-csv?al=1&market=socal&max_price=10000000&min_stories=1&num_homes=350&ord=days-on-redfin-asc&page_number=1&region_id={}&region_type={}&sf=1,2,3,5,6,7&status=131&uipt=1,2,3,4,5,6&v=8'.format(
            region_name_id, region_type_id)
        print('Quering Redfin from:\n{}'.format(url))

        response=urllib.request.urlopen(urllib.request.Request(url, headers=_REQUEST_HEADER))
        houses_csv=response.read().decode("utf-8")
        df_houses=convert_csv_to_dataframe(houses_csv)
        with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False, 'max_colwidth', -1):
            print(df_houses)

        redfin_res_full_dict=df_houses.to_dict('records')
    except:
        info = sys.exc_info()
        print(info)
    return redfin_res_full_dict#, region_name, region_mongo_id

def parse_region_type_name_in(region_type_name_in):
    rtn=region_type_name_in.replace('[', '').replace(']', '').split(':')
    region_type=rtn[0]
    region_names=rtn[1].split(',')
    return region_type, region_names


def get_property_sub_type(from_dict,listings):
    oldest_days_on_market = 0
    oldest_address = 'NA'
    ref_lis = 'NA'
    for lis in listings:
        listing_details = from_dict.get(lis)
        sampled_days_on_market = listing_details.get('days_on_market')
        if sampled_days_on_market > oldest_days_on_market:
            oldest_days_on_market = sampled_days_on_market
            sampled_address=listing_details.get('address')
            sampled_city=listing_details.get('city')
            sampled_zip_code=listing_details.get('zip_code')
            oldest_address = '{}, {}, {}'.format(sampled_address, sampled_city, sampled_zip_code)
            ref_lis = lis

    return oldest_days_on_market, oldest_address, ref_lis

def get_oldest(from_dict,listings):
    oldest_days_on_market = 0
    oldest_address = 'NA'
    ref_lis = 'NA'
    for lis in listings:
        listing_details = from_dict.get(lis)
        sampled_days_on_market = listing_details.get('days_on_market')
        if sampled_days_on_market > oldest_days_on_market:
            oldest_days_on_market = sampled_days_on_market
            sampled_address=listing_details.get('address')
            sampled_address=listing_details.get('address')
            sampled_city=listing_details.get('city')
            sampled_zip_code=listing_details.get('zip_code')
            oldest_address = '{}, {}, {}'.format(sampled_address, sampled_city, sampled_zip_code)
            ref_lis = lis

    return oldest_days_on_market, oldest_address, ref_lis

def get_db_mongo_config(envin, aws_secrets):
    if envin == 'prod':
        db_mongo = aws_secrets['qa-comp-proxy'].get('mongodb').get('uri')

    else:
        db_mongo = aws_secrets['comp-proxy'].get('mongodb').get('uri')

    return db_mongo

@pytest.mark.test_alignment_redfin
def test_active_redfin_vs_mongo(envin, region_type_name_in, aws_new):
    import sys

    class Insights_count:
        sampled_total = 0
        sampled_from_crmls=0
        sampled_from_mlslistings=0
        sampled_from_other_mls=0
        in_mongo_from_crmls = []
        in_mongo_from_mlslistings = []
        in_mongo_from_other_mls= []
        not_in_fact_from_crmls=0
        not_in_fact_from_mlslistings=0
        not_in_fact_from_other_mls=0


    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    global DB_MONGO,  DB_DATA_COMMON, DB_DATA_MLS_LISTINGS
    import matplotlib
    matplotlib.use('Agg')
    env_vars.env=envin
    start=time.time()
    redfin_res_dict_all={}
    redfin_duplicate = {}
    redfin_mis_by_region_source_dict={} #here we'll have the not in fact insights
    env_vars.redfin_count = 0
    total_missings = 0
    known_mls=['mlslistings', 'crmls']
    try:
        services = ['qa-comp-proxy', 'af-common-service', 'comp-proxy']

        aws_secrets = get_set_secrerts(envin,services)
        DB_MONGO=get_db_mongo_config(envin, aws_secrets)
        #DB_DATA_MLS_LISTINGS = set_postgress_db_from_secret_jdbc('af-merger', db_source='mlslisting', aws_secrets=aws_secrets)
        DB_DATA_COMMON=set_postgress_db_from_secret_jdbc('af-common-service', db_source='common', aws_secrets=aws_secrets)

        env_vars.summary={}
        env_vars.mis_lis = []
        env_vars.plus_lis=[]
        env_vars.duplicate_count = 0
        env_vars.postg_res_count = 0
        env_vars.mis_city=[]
        env_vars.env=envin
        start=time.time()
        white_dict={}



        res=False
        res_acc=[]
        dup_res={}

        # envin='prod'
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        logger=env_vars.logger
        # region_type_name_in = region_type_name
        env_vars.logger.info(
            'The Test is running on {0} env,\nComparing {3} Active/Pending listings on Redfin to Active/Pending on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
                envin, 'listings', THRSHOLD, region_type_name_in))

        region_type, region_names = parse_region_type_name_in(region_type_name_in)
        redfin_res_full_dict = {}
        mongo_res_full_dict = {}
        env_vars.region_type=region_type

        for region_name in region_names:
            region_type_name, region_type, region_name_id, region_type_id, region_mongo_id, region_name = get_redfin_mongo_region_type_name(logger,region_type, region_name)

            redfin_res = get_redfin_by(region_name_id, region_type_id)

            asy_res_redfin=get_act_redfin(region_name, region_type, redfin_res)
            redfin_res_dict=asy_res_redfin[0]
            for id, vals in redfin_res_dict.items():
                if redfin_res_dict_all.get(id):
                    redfin_duplicate[id]=[redfin_res_dict_all.get(id),vals]
                else:
                    redfin_res_dict_all[id]=vals
            redfin_res_by_source_dict=asy_res_redfin[1]
            env_vars.redfin_count+=len(redfin_res_dict)
            redfin_res_full_dict[region_name]=[redfin_res_dict, redfin_res_by_source_dict]
            # asy_res_mongo=get_act_mongo(envin, region_type=region_type,region_name=region_name, region_mongo_id=region_mongo_id)
            # mongo_res_dict=asy_res_mongo[0]
            # mongo_res_photos_count=asy_res_mongo[1]
            # mongo_res_full_dict[region_name]=[mongo_res_dict, mongo_res_photos_count]
            # env_vars.mongo_res_photos_count[region_name]=mongo_res_photos_count

        for region_name in region_names:
            region_type_name, region_type, region_name_id, region_type_id, region_mongo_id, region_name = get_redfin_mongo_region_type_name(logger,region_type, region_name)
            asy_res_mongo=get_act_mongo(envin, region_type=region_type, region_name=region_name,
                                        region_mongo_id=region_mongo_id)
            mongo_res_dict=asy_res_mongo[0]
            mongo_res_photos_count=asy_res_mongo[1]
            mongo_res_full_dict[region_name]=[mongo_res_dict, mongo_res_photos_count]
            env_vars.mongo_res_photos_count[region_name]=mongo_res_photos_count

        redfin_mongo_mis_by_region_dict={}
        env_vars.redfin_res_full_dict = redfin_res_full_dict
        print('Now testing MongoDB table')
        for region_name, region_vals in redfin_res_full_dict.items():
            region_type_name = '{}:{}'.format(region_type,region_name)
            print('Now testing {} on MongoDB table'.format(region_type_name))
            redfin_res_dict = region_vals[0]
            redfin_res_by_source_dict = region_vals[1]
            mongo_res_dict = mongo_res_full_dict.get(region_name)[0]
            Insights_count.sampled_total += len(redfin_res_dict)

            res, mis_lis, mis_by_mls=check_results_class.check_res_properties_redfin_insights(logger, region_type_name, redfin_res_dict,
                                                                         mongo_res_dict, redfin_res_by_source_dict,
                                                                         env_vars)
            res_acc.append(res)

            in_mongo=check_results_class.which_is_in(redfin_res_dict, mongo_res_dict)
            total_missings+=len(mis_lis)
            redfin_mongo_mis_by_region_dict[region_name] = {'mis_lis': mis_lis,'mis_by_mls':mis_by_mls}
            for inmo in in_mongo:
                mongo_redfin_source = redfin_res_dict.get(inmo)['source']
                if mongo_redfin_source.lower() in known_mls:
                    if mongo_redfin_source.lower() == 'crmls':
                        Insights_count.in_mongo_from_crmls.append(inmo)
                    elif mongo_redfin_source.lower() == 'mlslistings':
                        Insights_count.in_mongo_from_mlslistings.append(inmo)
                else:
                    Insights_count.in_mongo_from_other_mls.append(inmo)

            for src, sampled in redfin_res_by_source_dict.items():
                if  ....src.lower() in known_mls:
                    if  ....src.lower() == 'crmls':
                        Insights_count.sampled_from_crmls+= len(sampled)

                    elif  ....src.lower() == 'mlslistings':
                        Insights_count.sampled_from_mlslistings += len(sampled)

                else:
                    Insights_count.sampled_from_other_mls += len(sampled)




        for region_name, mis_lis_dict in redfin_mongo_mis_by_region_dict.items():
            mis_lis= mis_lis_dict.get('mis_lis')
            if len(mis_lis) == 0:
                continue
            mis_by_mls= mis_lis_dict.get('mis_by_mls')
            for src, mls_mis in mis_by_mls.items():
                asy_res_fact_specific=get_act_fact_listings(DB_DATA_COMMON, mls_mis)
                if len(asy_res_fact_specific) > 0:
                    insights = check_results_class.get_act_fact_insights(asy_res_fact_specific)

                    for key, val in insights.items():

                        if not getattr(Insights_count, '{}_{}'.format(src,key), 0):
                            setattr(Insights_count, '{}_{}'.format(src,key), len(val))
                        else:
                            setattr(Insights_count, '{}_{}'.format(src,key), getattr(Insights_count, '{}_{}'.format(src, key), 0)+len(val))

                    for key, val in insights.items():
                        if len(val)>0:
                            log_message_details='{}_{}_{}, Missing marked = {}\n{}\n{}\n'.format(region_name, src, key, len(val),list(val.keys()), val)
                            print('{}\n'.format(log_message_details))
                    for key, val in insights.items():

                        if len(val)>0:
                            log_message='{}: {}, Missing {} marked = {} listings'.format(region_name, src, key, len(val))
                            env_vars.logger.info(log_message)

                    mis_lis_dict={}
                    for i in mls_mis:
                        mis_lis_dict[i]=1
                    not_in_fact = check_results_class.which_not_in(asy_res_fact_specific, mis_lis_dict)
                    if  ....src.lower() in known_mls:
                        setattr(Insights_count,'not_in_fact_from_{}'.format( ....src.lower()), getattr(Insights_count, 'not_in_fact_from_{}'.format( ....src.lower()), 0)+len(not_in_fact))
                    else:
                        Insights_count.not_in_fact_from_other_mls+=len(not_in_fact)
                    if len(not_in_fact) > 0:
                        log_message='{}:{}, Missing Not in Fact = {}'.format(region_name,src, len(not_in_fact))
                        log_message_details='{}:{}, Missing Not in Fact Detailed = {}\n{}'.format(region_name,src, len(not_in_fact),
                                                                                               not_in_fact)
                        env_vars.logger.critical(log_message)
                        print('{}\n'.format(log_message_details))


                    for mis in not_in_fact:
                        for source, listings in redfin_res_full_dict[region_name][1].items():
                            if mis in listings:
                                if not redfin_mis_by_region_source_dict.get(region_name):
                                    redfin_mis_by_region_source_dict[region_name]={source:[mis]}
                                elif not redfin_mis_by_region_source_dict[region_name].get(source):
                                    redfin_mis_by_region_source_dict[region_name][source]=[mis]
                                else:
                                    redfin_mis_by_region_source_dict[region_name][source].append(mis)
                                break


                else:
                    not_in_fact=mls_mis
                    log_message = '{}: {}, All missing listings={} are not in Fact Table'.format(region_name, src,len(not_in_fact))
                    env_vars.logger.critical(log_message)
                    print(log_message)

                    if  ....src.lower() in known_mls:
                        setattr(Insights_count, 'not_in_fact_from_{}'.format( ....src.lower()),
                                getattr(Insights_count, 'not_in_fact_from_{}'.format( ....src.lower()), 0) + len(not_in_fact))
                    else:
                        Insights_count.not_in_fact_from_other_mls+=len(not_in_fact)
                    if len(not_in_fact) > 0:
                        log_message='{}: {}, Missing Not in Fact = {}'.format(region_name,src, len(not_in_fact))
                        log_message_details='{}:{}, Missing Not in Fact Detailed = {}\n{}'.format(region_name, src,
                                                                                               len(not_in_fact),
                                                                                               not_in_fact)
                        env_vars.logger.critical(log_message)
                        print('{}\n'.format(log_message_details))

                    # for mis in not_in_fact:
                    #     for source, listings in redfin_res_full_dict[region_name][1].items():
                    #         if mis in listings:
                    #             if not redfin_mis_by_region_source_dict.get(region_name):
                    #                 redfin_mis_by_region_source_dict[region_name]={source: [mis]}
                    #             elif not redfin_mis_by_region_source_dict[region_name].get(source):
                    #                 redfin_mis_by_region_source_dict[region_name][source]=[mis]
                    #             else:
                    #                 redfin_mis_by_region_source_dict[region_name][source].append(mis)
                    #             break
                    # continue


        logger.info('Total Missings = {}'.format(total_missings))
        print('Total Missings = {}\n'.format(total_missings))
        print('\n\n******* Not in Fact Distribution Datails **********:\n\n')
        if len(redfin_mis_by_region_source_dict)>0:
            for region_name, source_dist in redfin_mis_by_region_source_dict.items():
                log_message = '\n\n ***{} Not in Fact Datails ***:\n\n'.format(region_name)
                print(log_message)
                for source, listings in source_dist.items():
                    if source.lower() in known_mls:
                        if source.lower() =='crmls':
                            Insights_count.not_in_fact_from_crmls += len(listings)
                        elif source.lower() == 'mlslistings':
                            Insights_count.not_in_fact_from_mlslistings+=len(listings)
                        oldest_days_on_market, oldest_address, ref_lis = get_oldest(redfin_res_full_dict[region_name][0],listings)
                        log_message = '{}:{} Not in Fact:{} listings - Oldest listing is {} days old.\nRef Listing = {}, Listing Address={}'.format(region_name, source, len(listings), oldest_days_on_market,ref_lis, oldest_address)
                    log_details = '{}:{} Not in Fact Details:\n{}'.format(region_name, source, listings)
                    print(log_details)
                    logger.critical(log_message)
        else:
            log_message='All missings are in Fact Datails'
            print(log_message)
            logger.info(log_message)


        log_message='\n\n Redfin Number of listings sampled = {}\n\n'.format(env_vars.redfin_count)
        logger.info(log_message)
        print(log_message)
        for region_name, vals in redfin_res_full_dict.items():
            log_message = '{} sampled = {} listings'.format(region_name, len(vals[0]))
            logger.info(log_message)
            print(log_message)
        print(res)
        res_acc.append(res)

        # for vendor in vendors2_test:
        #     if vendor == 'sandicor':
        #         continue
        #
        #     async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))
        # pool.close()
        # pool.join()
        # async_result_conc.append([r.get() for r in async_result])
        #
        # for asy_res in async_result_conc[0]:
        #     no_assets=[]
        #     vendor=asy_res[0]
        #     postg_res_dict=asy_res[1]
        #     mongo_res_dict=asy_res[2]
        #     mongo_res_photos_count=asy_res[3].get(vendor)
        #     env_vars.mongo_res_photos_count[vendor]=mongo_res_photos_count
        #
        #     dup_res_dict=env_vars.dup_res_dict.get(vendor)
        #     res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
        #                                                       dup_res_dict, white_dict, env_vars)
        #     print(res)
        #     res_acc.append(res)
        #
        #     for key, val in mongo_res_photos_count.items():
        #         if val == 0:
        #             no_assets.append(key)
        #     env_vars.no_assets+=no_assets
        #
        #     if len(no_assets) > 0:
        #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
        #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))
        #         env_vars.no_assets+=no_assets
        #
        # res_acc.append(summary(test_type, envin))

        for region_name, records in mongo_res_full_dict.items():
            for listing_id, photo_count in records[1].items():
                if photo_count < 3:
                    src = records[0].get(listing_id)[0].get('mlsVendorType').replace('_', '').lower()
                    if not src in known_mls:
                        src = 'other_mls'
                    if listing_id in getattr(Insights_count,'in_mongo_from_{}'.format( ....src.lower())):
                        attr_name = 'missing_photos_from_sampled_{}'.format( ....src.lower())
                    else:
                        attr_name='missing_photos_from_{}'.format( ....src.lower())

                    src_listings_list = getattr(Insights_count, attr_name, [])
                    src_listings_list.append(listing_id)
                    setattr(
                        Insights_count,
                        attr_name,
                        src_listings_list
                            )


        print('****Insights Summary****\n')
        sum_message ='\n'
        sum_message_detailed='\n'

        #high level Insights:
        for attr, value in Insights_count.__dict__.items():
            if attr not in ('__module__','__dict__','__weakref__','__doc__'):
                if isinstance(value, list):
                    sum_message+='{} = {}\n'.format(attr, len(value))
                else:
                    sum_message+='{} = {}\n'.format(attr, value)

        for mls in ['mlslistings','crmls']:
            sampled_from_mls = getattr(Insights_count,'sampled_from_{}'.format(mls), 0)
            in_mongo_from_mls=getattr(Insights_count, 'in_mongo_from_{}'.format(mls), [])
            missing_photos = getattr(Insights_count, 'missing_photos_from_sampled_{}'.format(mls), [])

            if sampled_from_mls:
                mls_coverage_ratio = 100 * len(in_mongo_from_mls)/sampled_from_mls
                mls_valid_photos_coverage_ratio=100 * (1- len(missing_photos)/len(in_mongo_from_mls))
                sum_message += '{}_coverage_ratio = {}\n'.format(mls, mls_coverage_ratio)
                sum_message += '{}_valid_photos_coverage_ratio = {}\n'.format(mls, mls_valid_photos_coverage_ratio)

        logger.info(sum_message)
        print(sum_message)



        # Detailed Insights:
        for attr, value in Insights_count.__dict__.items():
            if attr not in ('__module__', '__dict__', '__weakref__', '__doc__'):
                if isinstance(value, list) and not re.search('in_mongo.*',attr):
                    sum_message_detailed += '{} = {}\n{}\n'.format(attr, len(value), value)
                elif isinstance(value, list) and re.search('in_mongo.*',attr):
                    sum_message_detailed += '{} = {}\n'.format(attr, len(value))
                else:
                    sum_message_detailed+='{} = {}\n'.format(attr, value)
        print(sum_message_detailed)


        if False in res_acc:
            res=False
        else:
            res=True
    except:
        res=False
        info=sys.exc_info()
        print(info)
        excp=traceback.format_exc()
        print(excp)
        # print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
    assert res

# @pytest.mark.test_alignment_redfin_back_up_last
# def test_active_redfin_vs_mongo_back_up_last(envin, region_type_name_in, aws_new):
#     import sys
#
#     class Insights_count:
#         not_in_fact=0
#         not_in_fact_crmls=0
#         not_in_fact_mlslistings=0
#
#     if str(aws_new).lower() != 'true':
#         aws_new=False
#     params.aws_new_account=aws_new
#     print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
#     global DB_MONGO,  DB_DATA_COMMON, DB_DATA_MLS_LISTINGS
#     import matplotlib
#     matplotlib.use('Agg')
#     env_vars.env=envin
#     start=time.time()
#     redfin_res_by_source_dict={}
#     redfin_mis_by_region_source_dict={} #here we'll have the not in fact insights
#     env_vars.redfin_count = 0
#     total_missings = 0
#     try:
#         services = ['qa-comp-proxy','af-common-service', 'comp-proxy']
#
#         aws_secrets = get_set_secrerts(envin,services)
#         DB_MONGO=get_db_mongo_config(envin, aws_secrets)
#         DB_DATA_MLS_LISTINGS = set_postgress_db_from_secret_jdbc('af-merger', db_source='mlslisting', aws_secrets=aws_secrets)
#         DB_DATA_COMMON=set_postgress_db_from_secret_jdbc('af-common-service', db_source='common', aws_secrets=aws_secrets)
#
#         env_vars.summary={}
#         env_vars.mis_lis = []
#         env_vars.plus_lis=[]
#         env_vars.duplicate_count = 0
#         env_vars.postg_res_count = 0
#         env_vars.mis_city=[]
#         env_vars.env=envin
#         start=time.time()
#         white_dict={}
#
#
#
#         res=False
#         res_acc=[]
#         dup_res={}
#
#         # envin='prod'
#         env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
#         logger=env_vars.logger
#         # region_type_name_in = region_type_name
#         env_vars.logger.info(
#             'The Test is running on {0} env,\nComparing {3} Active/Pending listings on Redfin to Active/Pending on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
#                 envin, 'listings', THRSHOLD, region_type_name_in))
#
#         region_type, region_names = parse_region_type_name_in(region_type_name_in)
#
#         redfin_res_full_dict = {}
#         mongo_res_full_dict = {}
#         env_vars.region_type=region_type
#
#         for region_name in region_names:
#             region_type_name, region_type, region_name_id, region_type_id, region_mongo_id, region_name = get_redfin_mongo_region_type_name(logger,region_type, region_name)
#
#             redfin_res = get_redfin_by(region_name_id, region_type_id)
#
#             asy_res_redfin=get_act_redfin(region_name, region_type, redfin_res)
#             redfin_res_dict=asy_res_redfin[0]
#             redfin_res_by_source_dict=asy_res_redfin[1]
#             env_vars.redfin_count+=len(redfin_res_dict)
#             redfin_res_full_dict[region_name]=[redfin_res_dict, redfin_res_by_source_dict]
#             asy_res_mongo=get_act_mongo(envin, region_type=region_type,region_name=region_name, region_mongo_id=region_mongo_id)
#             mongo_res_dict=asy_res_mongo[0]
#             mongo_res_photos_count=asy_res_mongo[1]
#             mongo_res_full_dict[region_name]=[mongo_res_dict, mongo_res_photos_count]
#             env_vars.mongo_res_photos_count[region_name]=mongo_res_photos_count
#
#         redfin_mongo_mis_by_region_dict={}
#         env_vars.redfin_res_full_dict = redfin_res_full_dict
#         print('Now testing MongoDB table')
#         for region_name, region_vals in redfin_res_full_dict.items():
#             region_type_name = '{}:{}'.format(region_type,region_name)
#             print('Now testing {} on MongoDB table'.format(region_type_name))
#             redfin_res_dict = region_vals[0]
#             redfin_res_by_source_dict = region_vals[1]
#             mongo_res_dict = mongo_res_full_dict.get(region_name)[0]
#             res, mis_lis=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict, mongo_res_dict,redfin_res_by_source_dict, env_vars)
#             #res, mis_lis, mis_by_mls=check_results_class.check_res_properties_redfin_insights(logger, region_type_name, redfin_res_dict,
#                                                                          # mongo_res_dict, redfin_res_by_source_dict,
#                                                                          # env_vars)
#             total_missings+=len(mis_lis)
#             redfin_mongo_mis_by_region_dict[region_name] = mis_lis
#             res_acc.append(res)
#
#
#
#         for region_name, mis_lis in redfin_mongo_mis_by_region_dict.items():
#             if len(mis_lis)==0:
#                 continue
#             asy_res_fact_specific=get_act_fact_listings(DB_DATA_COMMON, mis_lis)
#             if len(asy_res_fact_specific) > 0:
#                 insights=check_results_class.get_act_fact_insights(asy_res_fact_specific)
#                 # print('Missing reason indications:\n{}\n'.format(insights))
#                 for key, val in insights.items():
#                     if not getattr(Insights_count, '{}'.format(key), 0):
#                         setattr(Insights_count, '{}'.format(key), len(val))
#                     else:
#                         setattr(Insights_count, '{}'.format(key), getattr(Insights_count, '{}'.format(key), 0)+len(val))
#
#                 for key, val in insights.items():
#                     #if key !='valid_fact':
#                     log_message_details='{}: Missing {} marked = {}\n{}\n{}\n'.format(region_name, key, len(val),list(val.keys()), val)
#                     print('{}\n'.format(log_message_details))
#                 for key, val in insights.items():
#                     #if key !='valid_fact':
#                     if len(val)>0:
#                         log_message='{}: Missing {} marked = {} listings'.format(region_name, key, len(val))
#                         env_vars.logger.info(log_message)
#
#                 mis_lis_dict={}
#                 for i in mis_lis:
#                     mis_lis_dict[i]=1
#                 not_in_fact=check_results_class.which_not_in(asy_res_fact_specific, mis_lis_dict)
#                 Insights_count.not_in_fact+=len(not_in_fact)
#                 if len(not_in_fact) > 0:
#                     log_message='{}: Missing Not in Fact = {}'.format(region_name, len(not_in_fact))
#                     log_message_details='{}: Missing Not in Fact Detailed = {}\n{}'.format(region_name, len(not_in_fact),
#                                                                                            not_in_fact)
#                     env_vars.logger.critical(log_message)
#                     print('{}\n'.format(log_message_details))
#
#
#                 for mis in not_in_fact:
#                     for source, listings in redfin_res_full_dict[region_name][1].items():
#                         if mis in listings:
#                             if not redfin_mis_by_region_source_dict.get(region_name):
#                                 redfin_mis_by_region_source_dict[region_name]={source:[mis]}
#                             elif not redfin_mis_by_region_source_dict[region_name].get(source):
#                                 redfin_mis_by_region_source_dict[region_name][source]=[mis]
#                             else:
#                                 redfin_mis_by_region_source_dict[region_name][source].append(mis)
#                             break
#
#                 # if len(insights.get('valid_fact')) > 0:
#                 #     mis_lis_valid_dict=insights.get('valid_fact')
#                 #     mis_lis_valid_list=list(insights.get('valid_fact').keys())
#                 #     # not_in_fact_dict={}
#                 #     # for i in not_in_fact:
#                 #     #     not_in_fact_dict[i]=1
#                 #     mongo_res_dict, mongo_res_photos_count=get_act_mongo(envin, region_type='listing_ids', listing_ids=mis_lis_valid_list)
#                 #     not_in_mongo=check_results_class.which_not_in(mongo_res_dict, mis_lis_valid_dict)
#                 #     not_in_mongo_dict={}
#                 #     for i in not_in_mongo:
#                 #         not_in_mongo_dict[i]=1
#
#             else:
#                 log_message = '{}: All missing listings are not in Fact Table'.format(region_name)
#                 env_vars.logger.critical(log_message)
#                 print(log_message)
#                 not_in_fact=mis_lis
#                 Insights_count.not_in_fact+=len(not_in_fact)
#                 if len(not_in_fact) > 0:
#                     log_message='{}: Missing Not in Fact = {}'.format(region_name, len(not_in_fact))
#                     log_message_details='{}: Missing Not in Fact Detailed = {}\n{}'.format(region_name,
#                                                                                            len(not_in_fact),
#                                                                                            not_in_fact)
#                     env_vars.logger.critical(log_message)
#                     print('{}\n'.format(log_message_details))
#
#                 for mis in not_in_fact:
#                     for source, listings in redfin_res_full_dict[region_name][1].items():
#                         if mis in listings:
#                             if not redfin_mis_by_region_source_dict.get(region_name):
#                                 redfin_mis_by_region_source_dict[region_name]={source: [mis]}
#                             elif not redfin_mis_by_region_source_dict[region_name].get(source):
#                                 redfin_mis_by_region_source_dict[region_name][source]=[mis]
#                             else:
#                                 redfin_mis_by_region_source_dict[region_name][source].append(mis)
#                             break
#                 continue
#
#
#
#         # print('Now testing Fact table')
#         # asy_res_fact=get_act_fact(DB_DATA_COMMON)
#         # for region_name, region_vals in redfin_res_full_dict.items():
#         #     region_type_name='{}:{}'.format(region_type, region_name)
#         #     print('Now testing {} on Fact table'.format(region_type_name))
#         #     redfin_res_dict=region_vals[0]
#         #     redfin_res_by_sorce_dict=region_vals[1]
#         #     res=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict,
#         #                                                     asy_res_fact, redfin_res_by_sorce_dict, env_vars)
#
#
#         # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
#         #           "where not is_app_primary) t"
#
#         # mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter) t"
#         #
#         # # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
#         # postg_res=postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
#         #
#         # for res in postg_res:
#         #     mls_id = res[0].get('mlsid')
#         #     postg_res_dict[mls_id] = res[0].get('city')
#
#         # cities_white_list_query = "SELECT input_city, real_city, real_county FROM public.cities_dict where is_enabled"
#         # white_dict=enrichment_class.get_cities_counties_white_list(DB_GEO,
#         #                                                            cities_white_list_query)
#
#         logger.info('Total Missings = {}'.format(total_missings))
#         print('Total Missings = {}\n'.format(total_missings))
#         print('\n\n******* Not in Fact Distribution Datails **********:\n\n')
#         if len(redfin_mis_by_region_source_dict)>0:
#             for region_name, source_dist in redfin_mis_by_region_source_dict.items():
#                 log_message = '\n\n ***{} Not in Fact Datails ***:\n\n'.format(region_name)
#                 print(log_message)
#                 for source, listings in source_dist.items():
#                     if source in ('CRMLS','MLSISTINGS','MLSListings'):
#                         if source =='CRMLS':
#                             Insights_count.not_in_fact_crmls += len(listings)
#                         elif source == 'MLSListings':
#                             Insights_count.not_in_fact_mlslistings+=len(listings)
#                         oldest_days_on_market, oldest_address, ref_lis = get_oldest(redfin_res_full_dict[region_name][0],listings)
#                         log_message = '{}:{} Not in Fact:{} listings - Oldest listing is {} days old.\nRef Listing = {}, Listing Address={}'.format(region_name, source, len(listings), oldest_days_on_market,ref_lis, oldest_address)
#                     log_details = '{}:{} Not in Fact Details:\n{}'.format(region_name, source, listings)
#                     print(log_details)
#                     logger.critical(log_message)
#         else:
#             log_message='All missings are in Fact Datails'
#             print(log_message)
#             logger.info(log_message)
#
#
#         log_message='\n\n Redfin Number of listings sampled = {}\n\n'.format(env_vars.redfin_count)
#         logger.info(log_message)
#         print(log_message)
#         for region_name, vals in redfin_res_full_dict.items():
#             log_message = '{} sampled = {} listings'.format(region_name, len(vals[0]))
#             logger.info(log_message)
#             print(log_message)
#         print(res)
#         res_acc.append(res)
#
#         # for vendor in vendors2_test:
#         #     if vendor == 'sandicor':
#         #         continue
#         #
#         #     async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))
#         # pool.close()
#         # pool.join()
#         # async_result_conc.append([r.get() for r in async_result])
#         #
#         # for asy_res in async_result_conc[0]:
#         #     no_assets=[]
#         #     vendor=asy_res[0]
#         #     postg_res_dict=asy_res[1]
#         #     mongo_res_dict=asy_res[2]
#         #     mongo_res_photos_count=asy_res[3].get(vendor)
#         #     env_vars.mongo_res_photos_count[vendor]=mongo_res_photos_count
#         #
#         #     dup_res_dict=env_vars.dup_res_dict.get(vendor)
#         #     res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
#         #                                                       dup_res_dict, white_dict, env_vars)
#         #     print(res)
#         #     res_acc.append(res)
#         #
#         #     for key, val in mongo_res_photos_count.items():
#         #         if val == 0:
#         #             no_assets.append(key)
#         #     env_vars.no_assets+=no_assets
#         #
#         #     if len(no_assets) > 0:
#         #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
#         #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))
#         #         env_vars.no_assets+=no_assets
#         #
#         # res_acc.append(summary(test_type, envin))
#
#         print('****Insights Summary****\n')
#         sum_message ='\n'
#         for attr, value in Insights_count.__dict__.items():
#             if attr not in ('__module__','__dict__','__weakref__','__doc__'):
#                 sum_message += '{} = {}\n'.format(attr,value)
#
#         logger.info(sum_message)
#         print(sum_message)
#
#         if False in res_acc:
#             res=False
#         else:
#             res=True
#     except:
#         res=False
#         info=sys.exc_info()
#         print(info)
#         excp=traceback.format_exc()
#         print(excp)
#         # print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))
#
#     end=time.time()
#     print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
#     assert res

# @pytest.mark.test_alignment_redfin_back_up
# def test_active_redfin_vs_mongo_back_up(envin, region_type_name_in, aws_new):
#     import sys
#
#     if str(aws_new).lower() != 'true':
#         aws_new=False
#     params.aws_new_account=aws_new
#     print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
#     global DB_MONGO,  DB_DATA_COMMON, DB_DATA_MLS_LISTINGS
#     import matplotlib
#     matplotlib.use('Agg')
#     env_vars.env=envin
#     start=time.time()
#     redfin_res_by_source_dict={}
#     redfin_mis_by_region_source_dict={} #here we'll have the not in fact insights
#     env_vars.redfin_count = 0
#     try:
#         services = ['qa-comp-proxy','af-common-service', 'comp-proxy']
#
#         aws_secrets = get_set_secrerts(envin,services)
#         DB_MONGO=get_db_mongo_config(envin, aws_secrets)
#         DB_DATA_MLS_LISTINGS = set_postgress_db_from_secret_jdbc('af-merger', db_source='mlslisting', aws_secrets=aws_secrets)
#         DB_DATA_COMMON=set_postgress_db_from_secret_jdbc('af-common-service', db_source='common', aws_secrets=aws_secrets)
#
#         env_vars.summary={}
#         env_vars.mis_lis=[]
#         env_vars.plus_lis=[]
#         env_vars.duplicate_count=0
#         env_vars.postg_res_count=0
#         env_vars.mis_city=[]
#         env_vars.env=envin
#         start=time.time()
#         white_dict={}
#
#
#
#         res=False
#         res_acc=[]
#         dup_res={}
#
#         # envin='prod'
#         env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
#         logger=env_vars.logger
#         # region_type_name_in = region_type_name
#         env_vars.logger.info(
#             'The Test is running on {0} env,\nComparing {3} Active/Pending listings on Redfin to Active/Pending on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
#                 envin, 'listings', THRSHOLD, region_type_name_in))
#
#         region_type, region_names = parse_region_type_name_in(region_type_name_in)
#
#         redfin_res_full_dict = {}
#         mongo_res_full_dict = {}
#         env_vars.region_type=region_type
#
#         for region_name in region_names:
#             region_type_name, region_type, region_name_id, region_type_id, region_mongo_id = get_redfin_mongo_region_type_name(logger,region_type, region_name)
#
#             redfin_res = get_redfin_by(region_name_id, region_type_id)
#
#             asy_res_redfin=get_act_redfin(region_name, redfin_res)
#             redfin_res_dict=asy_res_redfin[0]
#             redfin_res_by_source_dict=asy_res_redfin[1]
#             env_vars.redfin_count+=len(redfin_res_dict)
#             redfin_res_full_dict[region_name]=[redfin_res_dict, redfin_res_by_source_dict]
#             asy_res_mongo=get_act_mongo(envin, region_type=region_type,region_name=region_name, region_mongo_id=region_mongo_id)
#             mongo_res_dict=asy_res_mongo[0]
#             mongo_res_photos_count=asy_res_mongo[1]
#             mongo_res_full_dict[region_name]=[mongo_res_dict, mongo_res_photos_count]
#             env_vars.mongo_res_photos_count[region_name]=mongo_res_photos_count
#
#         redfin_mongo_mis_by_region_dict={}
#         env_vars.redfin_res_full_dict = redfin_res_full_dict
#         print('Now testing MongoDB table')
#         for region_name, region_vals in redfin_res_full_dict.items():
#             region_type_name = '{}:{}'.format(region_type,region_name)
#             print('Now testing {} on MongoDB table'.format(region_type_name))
#             redfin_res_dict = region_vals[0]
#             redfin_res_by_source_dict = region_vals[1]
#             mongo_res_dict = mongo_res_full_dict.get(region_name)[0]
#             res, mis_lis=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict, mongo_res_dict,redfin_res_by_source_dict, env_vars)
#             redfin_mongo_mis_by_region_dict[region_name] = mis_lis
#             res_acc.append(res)
#
#
#
#         for region_name, mis_lis in redfin_mongo_mis_by_region_dict.items():
#             asy_res_fact_specific=get_act_fact_listings(DB_DATA_COMMON, mis_lis)
#             if len(asy_res_fact_specific) > 0:
#                 insights=check_results_class.get_act_fact_insights(asy_res_fact_specific)
#                 # print('Missing reason indications:\n{}\n'.format(insights))
#                 for key, val in insights.items():
#                     if key !='valid_fact':
#                         log_message_details='{}: Missing {} marked = {}\n{}'.format(region_name, key, len(val), val)
#                         print('{}\n'.format(log_message_details))
#                 for key, val in insights.items():
#                     if key !='valid_fact':
#                         log_message='{}: Missing {} marked = {} listings'.format(region_name, key, len(val))
#                         env_vars.logger.info(log_message)
#
#                 mis_lis_dict={}
#                 for i in mis_lis:
#                     mis_lis_dict[i]=1
#                 not_in_fact=check_results_class.which_not_in(asy_res_fact_specific, mis_lis_dict)
#                 if len(not_in_fact) > 0:
#                     log_message='{}: Missing Not in Fact = {}'.format(region_name, len(not_in_fact))
#                     log_message_details='{}: Missing Not in Fact Detailed = {}\n{}'.format(region_name, len(not_in_fact),
#                                                                                            not_in_fact)
#                     env_vars.logger.critical(log_message)
#                     print('{}\n'.format(log_message_details))
#
#
#                 for mis in not_in_fact:
#                     for source, listings in redfin_res_full_dict[region_name][1].items():
#                         if mis in listings:
#                             if not redfin_mis_by_region_source_dict.get(region_name):
#                                 redfin_mis_by_region_source_dict[region_name]={source:[mis]}
#                             elif not redfin_mis_by_region_source_dict[region_name].get(source):
#                                 redfin_mis_by_region_source_dict[region_name][source]=[mis]
#                             else:
#                                 redfin_mis_by_region_source_dict[region_name][source].append(mis)
#                             break
#
#                 # if len(insights.get('valid_fact')) > 0:
#                 #     mis_lis_valid_dict=insights.get('valid_fact')
#                 #     mis_lis_valid_list=list(insights.get('valid_fact').keys())
#                 #     # not_in_fact_dict={}
#                 #     # for i in not_in_fact:
#                 #     #     not_in_fact_dict[i]=1
#                 #     mongo_res_dict, mongo_res_photos_count=get_act_mongo(envin, region_type='listing_ids', listing_ids=mis_lis_valid_list)
#                 #     not_in_mongo=check_results_class.which_not_in(mongo_res_dict, mis_lis_valid_dict)
#                 #     not_in_mongo_dict={}
#                 #     for i in not_in_mongo:
#                 #         not_in_mongo_dict[i]=1
#
#             else:
#                 env_vars.logger.critical('{}: All missing listings are not in Fact Table'.format(region_name))
#
#
#
#         # print('Now testing Fact table')
#         # asy_res_fact=get_act_fact(DB_DATA_COMMON)
#         # for region_name, region_vals in redfin_res_full_dict.items():
#         #     region_type_name='{}:{}'.format(region_type, region_name)
#         #     print('Now testing {} on Fact table'.format(region_type_name))
#         #     redfin_res_dict=region_vals[0]
#         #     redfin_res_by_sorce_dict=region_vals[1]
#         #     res=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict,
#         #                                                     asy_res_fact, redfin_res_by_sorce_dict, env_vars)
#
#
#         # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
#         #           "where not is_app_primary) t"
#
#         # mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter) t"
#         #
#         # # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
#         # postg_res=postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
#         #
#         # for res in postg_res:
#         #     mls_id = res[0].get('mlsid')
#         #     postg_res_dict[mls_id] = res[0].get('city')
#
#         # cities_white_list_query = "SELECT input_city, real_city, real_county FROM public.cities_dict where is_enabled"
#         # white_dict=enrichment_class.get_cities_counties_white_list(DB_GEO,
#         #                                                            cities_white_list_query)
#
#         print('\n\n******* Not in Fact Distribution Datails **********:\n\n')
#         if len(redfin_mis_by_region_source_dict)>0:
#             for region_name, source_dist in redfin_mis_by_region_source_dict.items():
#                 log_message = '\n\n ***{} Not in Fact Datails ***:\n\n'.format(region_name)
#                 print(log_message)
#                 for source, listings in source_dist.items():
#                     log_message = '{}:{} Not in Fact:{} listings'.format(region_name, source, len(listings))
#                     log_details = '{}:{} Not in Fact Datails:\n{}'.format(region_name, source, listings)
#                     print(log_details)
#                     logger.critical(log_message)
#         else:
#             log_message='All missings are in Fact Datails'
#             print(log_message)
#             logger.info(log_message)
#
#
#         log_message='\n\n Redfin Number of listings sampled = {}\n\n'.format(env_vars.redfin_count)
#         logger.info(log_message)
#         print(log_message)
#         for region_name, vals in redfin_res_full_dict.items():
#             log_message = '{} sampled = {} listings'.format(region_name, len(vals[0]))
#             logger.info(log_message)
#             print(log_message)
#         print(res)
#         res_acc.append(res)
#
#         # for vendor in vendors2_test:
#         #     if vendor == 'sandicor':
#         #         continue
#         #
#         #     async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))
#         # pool.close()
#         # pool.join()
#         # async_result_conc.append([r.get() for r in async_result])
#         #
#         # for asy_res in async_result_conc[0]:
#         #     no_assets=[]
#         #     vendor=asy_res[0]
#         #     postg_res_dict=asy_res[1]
#         #     mongo_res_dict=asy_res[2]
#         #     mongo_res_photos_count=asy_res[3].get(vendor)
#         #     env_vars.mongo_res_photos_count[vendor]=mongo_res_photos_count
#         #
#         #     dup_res_dict=env_vars.dup_res_dict.get(vendor)
#         #     res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
#         #                                                       dup_res_dict, white_dict, env_vars)
#         #     print(res)
#         #     res_acc.append(res)
#         #
#         #     for key, val in mongo_res_photos_count.items():
#         #         if val == 0:
#         #             no_assets.append(key)
#         #     env_vars.no_assets+=no_assets
#         #
#         #     if len(no_assets) > 0:
#         #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
#         #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))
#         #         env_vars.no_assets+=no_assets
#         #
#         # res_acc.append(summary(test_type, envin))
#         if False in res_acc:
#             res=False
#         else:
#             res=True
#     except:
#         res=False
#         info=sys.exc_info()
#         print(info)
#         excp=traceback.format_exc()
#         print(excp)
#         # print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))
#
#     end=time.time()
#     print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
#     assert res
#
# @pytest.mark.test_alignment_redfin_Hold
# def test_active_redfin_vs_fact(envin, region_type_name_in, aws_new):
#     import sys
#     import matplotlib
#     matplotlib.use('Agg')
#     if str(aws_new).lower() != 'true':
#         aws_new=False
#     params.aws_new_account=aws_new
#     print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
#     global DB_MONGO,  DB_DATA_COMMON, DB_DATA_MLS_LISTINGS
#     try:
#         env_vars.env=envin
#         start=time.time()
#         res_acc=[]
#         if not env_vars.logger:
#             env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
#         logger=env_vars.logger
#         # region_type_name_in = region_type_name
#         env_vars.logger.info(
#             'The Test is running on {0} env,\nComparing {3} Active/Pending listings on Redfin to Active/Pending on {1}.\nIf a list was not found on {1}, it will be first verified if masked as duplicated on mlsds DB.\nOnly if not masked, it will be  marked as missing.\nTest will fail only if number of missing/surplus is greater then {2}.'.format(
#                 envin, 'listings', THRSHOLD, region_type_name_in))
#         if not env_vars.redfin_res_full_dict:
#
#             try:
#                 services = ['qa-comp-proxy','af-common-service', 'comp-proxy']
#
#                 aws_secrets = get_set_secrerts(envin,services)
#                 DB_MONGO=get_db_mongo_config(envin, aws_secrets)
#                 DB_DATA_MLS_LISTINGS = set_postgress_db_from_secret_jdbc('af-merger', db_source='mlslisting', aws_secrets=aws_secrets)
#                 DB_DATA_COMMON=set_postgress_db_from_secret_jdbc('af-common-service', db_source='common', aws_secrets=aws_secrets)
#
#                 env_vars.summary={}
#                 env_vars.mis_lis=[]
#                 env_vars.plus_lis=[]
#                 env_vars.duplicate_count=0
#                 env_vars.postg_res_count=0
#                 env_vars.mis_city=[]
#                 env_vars.env=envin
#                 start=time.time()
#                 white_dict={}
#
#
#
#                 res=False
#                 res_acc=[]
#                 dup_res={}
#
#                 # envin='prod'
#
#
#
#                 rtn = region_type_name_in.replace('[','').replace(']','').split(':')
#                 region_type = rtn[0]
#                 region_names = rtn[1].split(',')
#                 redfin_res_full_dict = {}
#                 mongo_res_full_dict = {}
#
#                 for region_name in region_names:
#                     region_type_name, region_type, region_name_id, region_type_id, region_mongo_id = get_redfin_mongo_region_type_name(logger,region_type, region_name)
#
#                     redfin_res = get_redfin_by(region_name_id, region_type_id)
#
#                     asy_res_redfin=get_act_redfin(region_name, redfin_res)
#                     redfin_res_dict=asy_res_redfin[0]
#                     redfin_res_by_source_dict=asy_res_redfin[1]
#                     redfin_res_full_dict[region_name]=[redfin_res_dict, redfin_res_by_source_dict]
#                     asy_res_mongo=get_act_mongo(envin, region_name=region_name, region_mongo_id=region_mongo_id)
#                     mongo_res_dict=asy_res_mongo[0]
#                     mongo_res_photos_count=asy_res_mongo[1]
#                     mongo_res_full_dict[region_name]=[mongo_res_dict, mongo_res_photos_count]
#                     env_vars.mongo_res_photos_count[region_name]=mongo_res_photos_count
#
#
#                 print('Now testing MongoDB table')
#                 for region_name, region_vals in redfin_res_full_dict.items():
#                     region_type_name = '{}:{}'.format(region_type,region_name)
#                     print('Now testing {} on MongoDB table'.format(region_type_name))
#                     redfin_res_dict = region_vals[0]
#                     redfin_res_by_source_dict = region_vals[1]
#                     mongo_res_dict = mongo_res_full_dict.get(region_name)[0]
#                     res=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict, mongo_res_dict,redfin_res_by_source_dict, env_vars)
#                     print(res)
#                     res_acc.append(res)
#
#
#             except:
#                 res=False
#                 info=sys.exc_info()
#                 print(info)
#                 excp=traceback.format_exc()
#                 print(excp)
#                 # print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))
#
#         redfin_res_full_dict=env_vars.redfin_res_full_dict
#         region_type=env_vars.region_type
#         print('Now testing Fact table')
#         asy_res_fact=get_act_fact_valid(DB_DATA_COMMON)
#         for region_name, region_vals in redfin_res_full_dict.items():
#             region_type_name='{}:{}'.format(region_type, region_name)
#             print('Now testing {} on Fact table'.format(region_type_name))
#             redfin_res_dict=region_vals[0]
#             redfin_res_by_source_dict=region_vals[1]
#             res, mis_lis=check_results_class.check_res_properties_redfin(logger, region_type_name, redfin_res_dict,
#                                                             asy_res_fact, redfin_res_by_source_dict, env_vars)
#
#
#             if len(mis_lis)>0:
#                 asy_res_fact_specific = get_act_fact_listings(DB_DATA_COMMON, mis_lis)
#                 if len(asy_res_fact_specific)>0:
#                     insights = check_results_class.get_act_fact_insights(asy_res_fact_specific)
#                     #print('Missing reason indications:\n{}\n'.format(insights))
#                     for key ,val in insights.items():
#                         log_message = '{}: Missing {} marked = {}'.format(region_type_name, key, len(val))
#                         log_message_details='{}: Missing {} marked = {}'.format(region_type_name, key, val)
#                         env_vars.logger.info(log_message)
#                         print('{}\n'.format(log_message_details))
#                     mis_lis_dict={}
#                     for i in mis_lis:
#                         mis_lis_dict[i] = 1
#                     not_in_fact = check_results_class.which_not_in(asy_res_fact_specific, mis_lis_dict)
#                     if len(not_in_fact)>0:
#                         log_message='{}: Missing Not in Fact = {} listings'.format(region_type_name, len(not_in_fact))
#                         log_message_details='{}: Missing Not in Fact Detailed = {}\n{}'.format(region_type_name, len(not_in_fact),not_in_fact)
#                         env_vars.logger.info(log_message)
#                         print('{}\n'.format(log_message_details))
#
#                 else:
#                     env_vars.logger.critical('{}: All missing listings are not in Fact Table = {} listings'.format(region_type_name), len(mis_lis))
#
#             # if len(mis_lis)>0:
#             #     asy_res_fact_specific = get_act_fact_listings(DB_DATA_COMMON, mis_lis)
#             #     if len(asy_res_fact_specific)>0:
#             #         insights = check_results_class.get_act_fact_insights(logger,asy_res_fact_specific)
#             #         print('Missing reason indications:\n{}\n'.format(insights))
#             #         for key ,val in insights.items():
#             #             log_message = '{}: Missing {} marked = {}'.format(region_type_name, key, len(val))
#             #             env_vars.logger.info(log_message)
#             #         #check_results_class.which_not_in()
#             #     else:
#             #         env_vars.logger.critical('{}: All missing listings are not in Fact Table'.format(region_type_name))
#
#
#
#
#
#
#
#             # mls_query="select row_to_json (t) from (select property_id, mls, duplicate_type, is_app_primary from metadata_tracking " \
#             #           "where not is_app_primary) t"
#
#             # mls_query="select row_to_json (t) from (SELECT * FROM public.fact_listings where status not in ('Withdrawn', 'Canceled', 'Closed', 'Expired', 'Delete') and not is_duplicated and not is_filter) t"
#             #
#             # # postg_res=postgress_class.get_query_psg(DB_GOLD, mls_query)
#             # postg_res=postgress_class.get_query_psg(DB_DATA_COMMON, mls_query)
#             #
#             # for res in postg_res:
#             #     mls_id = res[0].get('mlsid')
#             #     postg_res_dict[mls_id] = res[0].get('city')
#
#             # cities_white_list_query = "SELECT input_city, real_city, real_county FROM public.cities_dict where is_enabled"
#             # white_dict=enrichment_class.get_cities_counties_white_list(DB_GEO,
#             #                                                            cities_white_list_query)
#
#
#             print(res)
#             res_acc.append(res)
#
#             # for vendor in vendors2_test:
#             #     if vendor == 'sandicor':
#             #         continue
#             #
#             #     async_result.append(pool.apply_async(get_act_listings, args=(logger, envin, params, vendor, test_type)))
#             # pool.close()
#             # pool.join()
#             # async_result_conc.append([r.get() for r in async_result])
#             #
#             # for asy_res in async_result_conc[0]:
#             #     no_assets=[]
#             #     vendor=asy_res[0]
#             #     postg_res_dict=asy_res[1]
#             #     mongo_res_dict=asy_res[2]
#             #     mongo_res_photos_count=asy_res[3].get(vendor)
#             #     env_vars.mongo_res_photos_count[vendor]=mongo_res_photos_count
#             #
#             #     dup_res_dict=env_vars.dup_res_dict.get(vendor)
#             #     res=check_results_class.check_res_properties_diff(env_vars.logger, vendor, postg_res_dict, mongo_res_dict,
#             #                                                       dup_res_dict, white_dict, env_vars)
#             #     print(res)
#             #     res_acc.append(res)
#             #
#             #     for key, val in mongo_res_photos_count.items():
#             #         if val == 0:
#             #             no_assets.append(key)
#             #     env_vars.no_assets+=no_assets
#             #
#             #     if len(no_assets) > 0:
#             #         # env_vars.logger.critical('{}, {} listings with no assets info'.format(vendor, len(no_assets)))
#             #         print('{}, listings with no assets info:\n{}'.format(vendor, no_assets))
#             #         env_vars.no_assets+=no_assets
#             #
#             # res_acc.append(summary(test_type, envin))
#             if False in res_acc:
#                 res=False
#             else:
#                 res=True
#     except:
#         res=False
#         info=sys.exc_info()
#         print(info)
#         excp=traceback.format_exc()
#         print(excp)
#         # print('{}\nat{} line #{}'.format(info[1], info[2]['tb_frame'], info[2]['tb_lineno']))
#
#     end=time.time()
#     print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))
#     assert res
#

@pytest.mark.test_alignment
def test_active_wn_vs_mongo_summary(envin):
    start=time.time()
    try:
        res_acc=[]
        test_type='properties'

        res_acc.append(summary(test_type, envin))
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


