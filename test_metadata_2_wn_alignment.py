import sys
import time
from multiprocessing.pool import ThreadPool

import pytest

from .config import env_vars
from .config import params, gen_queries
from ... ....src.common.gen_config import gen_params
from ... ....src.common.tools.gen_features import check_results_class
from ... ....src.common.tools.aws.s3_aws import s3_class
from ... ....src.common.tools.db_tools.rets import rets_class

s3_class=s3_class.s3_class()
rets_class=rets_class.rets_class()

check_results_class=check_results_class.check_results_class()
from ... ....src.common.tools.db_tools.postgress_db import postgress_class

postgress_class=postgress_class.postgress_class()
from ... ....src.common.tools.gen_features.enrichment_class import enrichment

enrichment_class=enrichment()

test=None
logger=''
DAYS_PERIOD=365

vendors2_test=params.vendors2_test

mls_vendor_type=gen_params.mls_vendor_type
vendors_map=gen_params.vendors_map
vendors_filters=gen_params.vendors_filters

vendors_join_filters=gen_params.vendors_join_filters
vendors_join_filters_short=gen_params.vendors_join_filters_short

DB_GOLD=gen_params.DB_GOLD
DB_MONGO=gen_params.DB_MONGO
DB_WN=gen_params.DB_WN
THRSHOLD=500
START_DATE_SAMPLING='1970-01-01'
END_DATE_SAMPLING='2099-01-01'


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

def get_act_listings(logger, envin, params, vendor, test_type='properties', start_date_in=None, end_date_in=None):
    mls_res_dict={}
    try:
        all_from_meta={}

        test_type_queries=test_type

        if vendor not in ['sandicor']:
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
            mls_res_dict=rets_class.direct_properties_by_status_stream(vendor, rets_args, res_type='on_market',
                                                                       on_market=True)


        all_from_meta_query="select row_to_json (t) from (select property_id, is_app_primary, duplicate_type, mls, last_update_date, " \
                            "mapper_timestamp,normalizer_timestamp, basic_filter_timestamp, enrichment_timestamp, duplicate_cleaner_timestamp, " \
                            "mark_filter_timestamp from metadata_tracking " \
                            "where mls = '{}' " \
                            "and (" \
                            "mapper_timestamp is not null " \
                            "or duplicate_type is not null " \
                            "or is_app_primary is not null " \
                            "or normalizer_timestamp is not null " \
                            "or basic_filter_timestamp is not null " \
                            "or enrichment_timestamp is not null " \
                            "or duplicate_cleaner_timestamp is not null " \
                            "or mark_filter_timestamp is not null" \
                            ")" \
                            ")t".format(gen_params.mls_vendor_type.get(vendor))

        all_from_meta_res=postgress_class.get_query_psg(DB_GOLD[envin], all_from_meta_query)
        for res in all_from_meta_res:
            property_id=res[0].get('property_id')
            all_from_meta[property_id]=res
        env_vars.all_from_meta[vendor]=all_from_meta

    except:
        info=sys.exc_info()
        print(info)
        mls_res_dict={}
        all_from_meta={}

    return vendor, mls_res_dict, all_from_meta


def summary(test_type, envin):
    if test_type and test_type in ['properties_24', 'properties_time_frame']:
        env_vars.plus_lis=[]
    res=False
    env_vars.logger.info(
        '\nTest summary: WN Vs Mongo - {} Environemnt.\nFor full report, review "test_active_wn_vs_metadata"'.format(
            envin))
    mis_alignment_ratio=0
    if env_vars.postg_res_count:
        mis_alignment_ratio=100 * (len(env_vars.mis_lis) + len(env_vars.broken_lis)) / env_vars.postg_res_count
    else:
        mis_alignment_ratio=None

    message='\nTotal in WN = {},\nMis alignment ratio = {}%,\nTotal missing = {},\nTotal Broken = {},\nmissed_cities = {}'.format(
        env_vars.postg_res_count, mis_alignment_ratio, len(env_vars.mis_lis), len(env_vars.broken_lis),
        len(env_vars.mis_city))

    if mis_alignment_ratio > 1 or len(env_vars.mis_lis) > THRSHOLD or len(env_vars.broken_lis) > THRSHOLD:
        env_vars.logger.critical(message)
        print('Test Fail:\n')
        print(message)

    else:
        res=True
        env_vars.logger.info(message)
        print('Test Pass:\n')
        print(message)

    return res


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


@pytest.mark.test_alignment
def test_active_wn_vs_meta_data(envin, aws_new):
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
    try:
        env_vars.logger=startLogger(logPath='ETL_e2e_test_{}.log'.format(envin))
        DB_MONGO, DB_GOLD, DB_WN, DB_RETS=get_set_secrerts(envin)
        res=False
        res_acc=[]
        dup_res={}

        # envin='prod'

        env_vars.logger.info(
            'The Test is running on {0} env, comparing Active listings on Source MLS to Records on {1}.\nIf a list was not found on {1}, it will be   marked as missing.\nThe Test will indicate about the number of broken records on {1}.\nBroken records disribes a valid cities listings with NULL value on either [is_app_primary, mapped, enrichned, duplicate_cleaner].\nTest will fail only if number of missing/surplus is greater then {2}.\n\n'.format(
                envin, 'meta_data_tracking', THRSHOLD))
        pool=ThreadPool(processes=2)
        async_result=[]
        async_result_conc=[]
        test_type='properties'

        cities_white_list_query="SELECT * FROM location_dict where is_enabled"
        white_dict=enrichment_class.get_cities_counties_white_list(DB_GOLD[envin],
                                                                   cities_white_list_query)

        for vendor in vendors2_test:
            # if vendor == 'sandicor':
            #     continue

            async_result.append(pool.apply_async(get_act_listings, args=(
                logger, envin, params, vendor, test_type)))
        pool.close()
        pool.join()
        async_result_conc.append([r.get() for r in async_result])

        for asy_res in async_result_conc[0]:
            no_assets=[]
            vendor=asy_res[0]
            # wn_res_dict=asy_res[1]
            #all_from_meta_dict=asy_res[2]

            dup_res_dict=env_vars.dup_res_dict.get(vendor)
            res=check_results_class.check_res_metadata_diff(env_vars.logger, vendor, asy_res[1], asy_res[2],
                                                            white_dict, env_vars)
            print(res)
            res_acc.append(res)

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
    assert True


@pytest.mark.test_alignment
def test_active_wn_vs_meta_data_summary(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
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
