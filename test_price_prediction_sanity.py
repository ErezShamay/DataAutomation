import datetime
import re
import sys
import time

import pytest
from pymongo import MongoClient

from .config import env_vars, params
from ... ....src.common.gen_config import gen_params
from ... ....src.common.tools.aws.s3_aws import s3_class
from ... ....src.common.tools.db_tools.mongo_db import mongo_class

s3_class=s3_class.s3_class()

mongo_class=mongo_class.mongo_class()
DB_MONGO=gen_params.DB_MONGO

PP_COVERAGE_THRESHOLD=0.7
DAYS_PERIOD=30
MAX_PRED_TO_PRICE_RATIO=0.15
MAX_PP_LISTINGS_SAMPLES=20000


def get_logger(envin):
    if not getattr(env_vars, 'logger', None):
        logger=env_vars.logger=startLogger(logPath='price_prediction_health_test_{}.log'.format(envin))
    else:
        logger=env_vars.logger
    return logger


def set_mongo_query(query_key, ids=[], time_back_epoch=1483228800):
    queries={

        'active': {
            "currentListing.listingStatus": {'$exists': True, "$in": ["ACTIVE", "CONTINGENT", "PENDING"]},
            "location.address1": {'$ne': 'Undisclosed Address'},
            "createdAt": {"$gte": time_back_epoch}},  # 2019-01-01

        'active_pp': {
            "currentListing.listingStatus": {'$exists': True, "$in": ["ACTIVE", "CONTINGENT", "PENDING"]},
            "location.address1": {'$ne': 'Undisclosed Address'},
            "hasOfferPrediction": True,
            "createdAt": {"$gte": time_back_epoch}},  # 2019-01-01

        'active_offer': {'listingId': {'$in': ids}}
    }
    query=queries[query_key]

    return query


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

def count_query(envin, query_type, time_back_epoch):
    full_query=set_mongo_query(query_type, time_back_epoch=time_back_epoch)
    db_mongo_args=get_db_mongo_args(envin)

    connect=None
    if envin == 'prod' and hasattr(gen_params,
                                   'vendors_on_real_prod') and env_vars.vendor not in gen_params.vendors_on_real_prod:
        client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                           port=27017)
        db=client['realidb-production']
        connect=db['listings']
    mongo_res=mongo_class.get_count_query_mongo(db_mongo_args, full_query, connect)

    act_cnt=mongo_res
    return act_cnt


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

def get_db_mongo_config(envin, aws_secrets):
    if envin == 'prod':
        db_mongo = aws_secrets['qa-comp-proxy'].get('mongodb').get('uri')

    else:
        db_mongo = aws_secrets['comp-proxy'].get('mongodb').get('uri')

    return db_mongo

def get_set_secrerts(envin):
    db_mongo={}
    db_gold={}
    db_wn={}
    try:
        services=['qa-comp-proxy','de-extractor', 'de-saver', 'comp-proxy']
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf=s3_class.get_secrets_by_service(envin, service_name, params)
                env_vars.aws_secrets[service_name]=conf

        db_gold[envin]=set_postgress_db_from_secret_jdbc('de-saver', db_source='rds')
        db_wn=set_postgress_db_from_secret_jdbc('de-extractor', db_source='wolfnet')
        db_mongo=get_db_mongo_config(envin, env_vars.aws_secrets)
    except:
        info=sys.exc_info()
        print(info)

    return db_mongo, db_gold, db_wn

def check_pp_values(logger, env_vars, offers, ext_ids_has_offer_to_price):
    res_acc=[]

    high_prediction_to_act_ratio={}
    fault_prediction_range={}

    if len(offers) > 0:
        for res_p in offers:
            min_pred_prob=90
            min_pred_amount=0
            predicted_price=res_p.get('pricePrediction').get('price')
            listing_external_id=res_p.get('listingId')
            asked_price=ext_ids_has_offer_to_price.get(listing_external_id).get('listing_price')
            prediction_to_asked_ratio=round(predicted_price / asked_price, 3)
            prediction_range=res_p.get('predictions')
            updated_at=res_p.get('updatedAt')
            address=ext_ids_has_offer_to_price.get(listing_external_id).get('address')
            if prediction_to_asked_ratio < 1 - MAX_PRED_TO_PRICE_RATIO or prediction_to_asked_ratio > 1 + MAX_PRED_TO_PRICE_RATIO:
                high_prediction_to_act_ratio[listing_external_id]={'ratio': prediction_to_asked_ratio,
                                                                   'predicted_price': predicted_price,
                                                                   'asked_price': asked_price, 'updatedAt': updated_at,
                                                                   'address': address}
            for pred_prob in prediction_range:
                amount=pred_prob.get('amount')
                prob=pred_prob.get('probability')
                if amount > min_pred_amount:
                    min_pred_amount=amount
                else:
                    fault_prediction_range[listing_external_id]={'amount'}
                if prob > min_pred_prob:
                    min_pred_prob=prob
                else:
                    fault_prediction_range[listing_external_id]={'prob'}

    else:
        log_message='*****No offer prediction was found*****'
        logger.critical(log_message)
        print(log_message)
        res_acc.append(False)

    log_message='#{} listings out of {} listings sampled are with Fault prediction/asking_price ratio > {} Threshold'.format(
        len(high_prediction_to_act_ratio), len(offers), MAX_PRED_TO_PRICE_RATIO)
    if len(high_prediction_to_act_ratio) > 0:
        res_acc.append(False)
        logger.critical(log_message)
        print(log_message)
        for key, val in high_prediction_to_act_ratio.items():
            print(
                '{}: Fault ratio={}, Asked Price={}, Predicted Price={}, Updated at={}, Address={} '.format(key,
                                                                                                            val.get(
                                                                                                                'ratio'),
                                                                                                            val.get(
                                                                                                                'asked_price'),
                                                                                                            val.get(
                                                                                                                'predicted_price'),
                                                                                                            val.get(
                                                                                                                'updatedAt'),
                                                                                                            val.get(
                                                                                                                'address')))

    else:
        res_acc.append(True)
        logger.info(log_message)
        print(log_message)

    log_message='#{} listings out of {} listings sampled are with Fault prediction range'.format(
        len(fault_prediction_range),
        len(offers))

    if len(fault_prediction_range) > 0:
        res_acc.append(False)
        logger.critical(log_message)
        print(log_message)
        for key, val in fault_prediction_range.items():
            print('{}: {}'.format(key, val))
    else:
        res_acc.append(True)
        logger.info(log_message)

    if len(res_acc) > 0 and False not in res_acc:
        res=True
    else:
        res=False

    env_vars.summary['fault_prediction_val']='{}: sampled={}'.format(
        len(high_prediction_to_act_ratio) / len(offers),
        len(offers))
    env_vars.summary['fault_prediction_range']='{}: sampled={}'.format(len(fault_prediction_range) / len(offers),
                                                                       len(offers))
    return res


@pytest.mark.test_price_prediction_sanity
def test_price_prediction_coverage(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    global DB_MONGO, DB_GOLD, DB_WN
    DB_MONGO, DB_GOLD, DB_WN=get_set_secrerts(envin)
    env_vars.env=envin

    time_back=datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
    time_back_epoch=int(time_back.timestamp())

    start=time.time()

    logger=get_logger(envin)
    logger.info(
        "{} Env, The test is verifying the ratio between active listings To active with price prediction,\nExpecting minimum ratio of {}.\nTime frame is for listings created during the last {} days.".format(
            envin, PP_COVERAGE_THRESHOLD, DAYS_PERIOD))

    active_cnt=count_query(envin, query_type='active', time_back_epoch=time_back_epoch)
    active_pp_cnt=count_query(envin, query_type='active_pp', time_back_epoch=time_back_epoch)
    # if not active_pp_cnt or not active_cnt:
    #     logger.critical('Fail to get listings count')
    #     assert False

    pp_ratio=0
    if active_cnt > 0:
        pp_ratio=active_pp_cnt / active_cnt

    log_message='Active with price prediction to all Actives ratio is {}, Threshold={}.\n{} Active listings were sampled\n{} Active listings (Max={}) with price prediction are to be used for further testing'.format(
        pp_ratio, PP_COVERAGE_THRESHOLD, active_cnt, active_pp_cnt, MAX_PP_LISTINGS_SAMPLES)
    if pp_ratio < PP_COVERAGE_THRESHOLD:
        logger.critical(log_message)
        print(log_message)
        res=False
    else:
        logger.info(log_message)
        print(log_message)
        res=True

    env_vars.summary['price_prediction_coverage']='{}: sampled={}'.format(pp_ratio, active_cnt)

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))

    assert res


@pytest.mark.test_price_prediction_sanity
def test_price_prediction_values(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    env_vars.env=envin

    time_back=datetime.datetime.now() - datetime.timedelta(days=DAYS_PERIOD)
    time_back_epoch=int(time_back.timestamp())
    start=time.time()

    logger=get_logger(envin)
    logger.info(
        "{} Env, The test is verifing that listings with Offer Prediction has a legit values\nExpecting prediction/asking price ratio of max {}.\nExpecting prediction range to be correlated to probabilty (the higher price offering, the higher probabilty for winning)\nTime frame is for listings created during the last {} days.".format(
            envin, MAX_PRED_TO_PRICE_RATIO, DAYS_PERIOD))


    db_mongo_args=get_db_mongo_args(envin)
    ext_ids_has_offer_to_price={}

    query_mongo={'currentListing.listingStatus': {'$exists': True, '$nin': ['UNKNOWN', 'MLS_SOLD', 'OFF-MARKET']},
                 'location.address1': {'$ne': 'Undisclosed Address'},
                 'hasOfferPrediction': True,
                 'createdAt': {'$gte': time_back_epoch}
                 }
    projection={"item1": 1,
                "external_id": "$currentListing.listingExternalId",
                "item2": 1,
                "price": "$currentListing.price",
                "item3": 1,
                "vendorMlsId": "$currentListing.vendorMlsId",
                "item4": 1,
                "realiUpdated": "$realiUpdated",
                "item5": 1,
                "listingStatus": "$currentListing.listingStatus",
                "listDate": "$currentListing.listDate",
                "item6": 1,
                "mlsVendorType": "$currentListing.mlsVendorType",
                "item7": 1,
                "createdAt": "$createdAt",
                "item8": 1,
                "address": "$location.address1"
                }
    full_query=[
        {
            "$match": query_mongo
        },
        {"$project": projection
         },
        {
            "$sort": {"createdAt": -1}
        }
    ]

    connect=None
    if envin == 'prod' and hasattr(gen_params,
                                   'vendors_on_real_prod') and env_vars.vendor not in gen_params.vendors_on_real_prod:
        client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                           port=27017)
        db=client['realidb-production']
        connect=db['listings']
    mongo_res=mongo_class.get_aggregate_query_mongo(db_mongo_args, full_query, connect)

    res_count=0  # mongo_res.count()
    if mongo_res:
        res_count=len(mongo_res)
    if res_count == 0:
        logger.critical('Fail to get listings with {}'.format(full_query))
        print('Fail to get listings with {}'.format(full_query))
        assert False

    res_acc=[]

    for res in mongo_res:
        vendor_mls_id=res.get('vendorMlsId')
        listing_external_id=res.get('external_id')
        listing_price=res.get('price')
        mls_vendor_name=res.get('mlsVendorType')
        address=res.get('address')
        if vendor_mls_id and listing_external_id and listing_price:
            ext_ids_has_offer_to_price[listing_external_id]={'listing_price': listing_price,
                                                             'vendor_mls_id': vendor_mls_id,
                                                             'mls_vendor_name': mls_vendor_name, 'address': address}
        else:
            logger.critical(
                '{}: Broken offer prediction for listing externalid = {}, listing price = {}'.format(vendor_mls_id,
                                                                                                     listing_external_id,
                                                                                                     listing_price))

    ids=list(ext_ids_has_offer_to_price.keys())

    full_query=set_mongo_query('active_offer', ids[0:MAX_PP_LISTINGS_SAMPLES])
    db_mongo_args=get_db_mongo_args(envin, collection='OfferPrediction')

    connect=None
    if envin == 'prod' and hasattr(gen_params,
                                   'vendors_on_real_prod') and env_vars.vendor not in gen_params.vendors_on_real_prod:
        client=MongoClient("mongodb://RealiDB:g9CBgF0GYwdSTwa6@comp-ghost-shard-00-00.kziwr.mongodb.net", ssl=True,
                           port=27017)
        db=client['realidb-production']
        connect=db['OfferPrediction']
    mongo_res=mongo_class.get_find_query_mongo(db_mongo_args, full_query, connect)

    res=check_pp_values(logger, env_vars, mongo_res, ext_ids_has_offer_to_price)

    end=time.time()
    print("{} - time factor is {}".format(sys._getframe().f_code.co_name, end - start))

    assert res


@pytest.mark.test_price_prediction_sanity
def test_price_prediction_summary(envin):
    env_vars.env=envin
    logger=get_logger(envin)
    for key, val in env_vars.summary.items():

        log_message='{}: {}'.format(key, val)
        logger.info(log_message)

    assert True