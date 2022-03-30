import json
import os
import re
import sys

import jsonschema
import pytest

from .config import env_vars, params
from ... ....src.common.gen_config import gen_params
from ... ....src.common.gen_config.gen_mapping import genmapping
from ... ....src.common.tools.gen_features import res_by_id_class
from ... ....src.common.tools.gen_features.check_results_class import check_results_class
from ... ....src.common.tools.gen_features.enrichment_class import enrichment
from ... ....src.common.tools.gen_features.mapper import sandicor_properties_mapper, sandicor_photos_mapper, sandicor_open_house_mapper, \
    wn_properties_mapper
from ... ....src.common.tools.gen_features.normalizator import val_normalizator
from ... ....src.common.tools.aws.s3_aws import s3_class
from ... ....src.common.tools.db_tools.mongo_db import mongo_class
from ... ....src.common.tools.db_tools.postgress_db import postgress_class
from ... ....src.common.tools.gen_features import set_environment, set_file_name  # To be Re check location
from ... ....src.common.tools.gen_features.file_edit_class import file_edit
from ... ....src.common.tools.gen_features.mapper import get_first
from ... ....src.common.tools.gen_features.mock_data_class import mock_data
from ..mapper.test_env.gen_edit import gen_properties_wn_key_edit, gen_properties_sandicor_key_edit
from ... ....src.common.tools.db_tools.redis_db import redis_class

enrichment_class=enrichment()

res_by_id_class=res_by_id_class.res_by_id()

DB_WN=gen_params.DB_WN

val_normalizator=val_normalizator()
genmapping=genmapping()
check_results_class=check_results_class()


mock_data=mock_data()
file_edit=file_edit()
s3_class=s3_class.s3_class()
redis_class=redis_class.redis_class()
postgress_class=postgress_class.postgress_class()
mongo_class=mongo_class.mongo_class()

DB_GOLD=gen_params.DB_GOLD
DB_MONGO=gen_params.DB_MONGO

root_dir=os.path.dirname(os.path.realpath(__file__))
megatron_root_dir=os.path.dirname(os.path.realpath('.'))


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



def set_query_psg(query_type, table=None, list_for_in_query=None):
    ids_str=''
    if list_for_in_query:
        ids=list_for_in_query

        for i in range(len(ids)):
            if i == len(ids) - 1:
                ids_str+="'{}'".format(ids[i])

            else:
                ids_str+="'{}',".format(ids[i])

    query_test_types={
        'cities_white_list':
            "SELECT input_city, real_city, real_county "
            "FROM location_dict where is_enabled",
        'properties_all':
            "select row_to_json (t) from (select * from {}) t".format(table),
        'properties_listing_ids':
            "select row_to_json (t) from (select * from {} where property_id in ({}) order by mls_update_date) t".format(
                table, ids_str),
        'photos_listing_ids':
            "select row_to_json (t) from (select * from {} where property_id in ({})) t".format(
                table, ids_str),
        'open_house_listing_ids':
            "select row_to_json (t) from (select * from {} where property_id in ({})) t".format(
                table, ids_str),
        'meta_data_query_back': "SELECT row_to_json (t) from "
                                "(select "
                                "req_id, mapped_id, property_id, reali_mls, normaliser_version, normaliser_timestamp, identifier,"
                                "entered_timestamp, is_primary, base_filter_version, base_filter_timestamp, enrich_version, enrich_timestamp,"
                                "duplicate_cleaner_version, duplicate_cleaner_timestamp, mark_filter_version, mark_filter_timestamp, has_photos,"
                                "photos_count, has_openhouse "
                                "FROM metadata_tracking) t",
        'meta_data_query':
            "select row_to_json (t) from (select * from metadata_tracking where property_id in ({})) t".format(
                ids_str),
        'normalized_data_query': "SELECT row_to_json (t) from "
                                 "(select * FROM {} where property_id in ({})) t".format(table, ids_str),
        'delete_listing_ids': "DELETE FROM {} WHERE property_id in ({})".format(table, ids_str),
        'enriched_location_cache': "select row_to_json (t) from (SELECT * FROM location_cache where zip_code is not null and neighborhood is not null and address is not null and lat is not null and lng is not null and real_zip_code is not null and real_county is not null and accuracy = 'ROOFTOP' and address not ilike '0%' and address !='' and city in {} limit 15000) t".format(
            list_for_in_query)

    }
    query=query_test_types[query_type]
    return query


def get_s3_bucket_name(envin):
    base_bucket='mls-raw-data'
    if envin in ['dev', 'qa', 'staging']:
        bucket='{}-{}'.format(envin, base_bucket)
    elif envin == 'prod':
        bucket='production-{}'.format(base_bucket)
    else:
        bucket=None
    return bucket


def set_expected(last_records, folder=None, json_file=None, db=None):  # To be reused

    # set expected map
    expected_map={}
    for id_key, val in last_records.items():

        if not val:
            env_vars.logger.critical('No data found for {}'.format(id_key))
            continue
        vendor, listing_id, housetype=res_by_id_class.get_id_key_details(id_key)
        # if vendor == 'sandicor':
        #     mls_map=getattr(mapper_mappings, '{}2wn_map_{}'.format(vendor, housetype))
        #     for mls_key, mls_val in val['data'].items():
        #         mapped_key=mls_map.get(mls_key)
        #         if not mapped_key:
        #             continue
        #         if expected_map.get(id_key):
        #             expected_map[id_key]['data'][mapped_key]=mls_val
        #         else:
        #             expected_map[id_key]={'data': {mapped_key: mls_val}, 'reali_meta_data': {}}
        #     expected_map[id_key]['reali_meta_data']=val['reali_meta_data']
        #     env_vars.expected_map[id_key]=expected_map[id_key]

        if vendor == 'sandicor':
            mapped={}
            if folder == 'properties':
                mapped=sandicor_properties_mapper(val.get('data'))
            elif folder == 'photos':
                mapped=sandicor_photos_mapper(val.get('data'))
            elif folder == 'openhouse':
                mapped=sandicor_open_house_mapper(val.get('data'))

            mapped_structure={'data': mapped, 'reali_meta_data': val.get('reali_meta_data')}
            if not expected_map.get(id_key):
                expected_map[id_key]=[mapped_structure]
            else:
                expected_map[id_key].append(mapped_structure)

            # env_vars.expected_map[id_key]=expected_map[id_key]

            # mls_map=getattr(mapper_mappings, 'wn2{}_map'.format(vendor, housetype))
            # for mls_key, mapped_key in mls_map.items():
            #     mapped_val = val['data'].get(mapped_key)
            #     #
            #     # mapped_key=mls_map.get(mls_key)
            #     # if not mapped_key:
            #     #     continue
            #     if expected_map.get(id_key):
            #         expected_map[id_key]['data'][mls_key]=mapped_val
            #     else:
            #         expected_map[id_key]={'data': {mls_key: mapped_val}, 'reali_meta_data': {}}
            # expected_map[id_key]['reali_meta_data']=val['reali_meta_data']
            # env_vars.expected_map[id_key]=expected_map[id_key]
        else:
            mapped={}
            if folder == 'properties':
                mapped=wn_properties_mapper(val.get('data'))
                val_data=val.get('data')
                # if val_data:
                #     ref_key_mapped=sandicor_properties_mapper(val_data)#use only keys to be mapped
                #     for r_key in list(ref_key_mapped.keys()):
                #         mapped[r_key] = val_data.get(r_key)
                mapped_structure={'data': mapped, 'reali_meta_data': val.get('reali_meta_data')}
                if not expected_map.get(id_key):
                    expected_map[id_key]=[mapped_structure]
                else:
                    expected_map[id_key].append(mapped_structure)

        if len(mapped) == 0:
            if not expected_map.get(id_key):
                expected_map[id_key]=[val]
            else:
                expected_map[id_key].append(val)

        if expected_map.get(id_key):
            env_vars.expected_map[id_key]=expected_map[id_key]
        else:
            env_vars.expected_map[id_key]=[{'data': {}, 'reali_meta_data': {}}]

    # set expected norm
    expected_norms={}
    normalized_map={}
    for mapped_id_key, mapped_val_list in expected_map.items():
        for mapped_val in mapped_val_list:
            if not mapped_val.get('data'):
                continue

        if folder:
            if folder == 'properties':
                normalized_map=val_normalizator.get_normalized_properties(env_vars, mapped_id_key, norm_val=None, db=db)
            elif folder == 'photos':
                normalized_map=val_normalizator.get_normalized_photos(env_vars, mapped_id_key, norm_val=None)
            elif folder == 'openhouse':
                normalized_map=val_normalizator.get_normalized_openhouse(env_vars, mapped_id_key, norm_val=None)

        expected_norms[mapped_id_key]=normalized_map
        env_vars.expected_norms[mapped_id_key]=normalized_map

    # set expected app
    expected_app={}
    normalized_app={}
    for mapped_id_key, mapped_val_list in expected_map.items():
        for mapped_val in mapped_val_list:
            if not mapped_val.get('data'):
                continue

        if folder:
            if folder == 'properties':
                normalized_app=val_normalizator.get_mongo_normalized_properties(env_vars, mapped_id_key,
                                                                                norm_val=None, db=db)
            elif folder == 'photos':
                normalized_app=val_normalizator.get_mongo_normalized_photos(env_vars, mapped_id_key, norm_val=None)
            elif folder == 'openhouse':
                normalized_app=val_normalizator.get_mongo_normalized_openhouse(env_vars, mapped_id_key,
                                                                               norm_val=None)

        expected_app[mapped_id_key]=normalized_app
        env_vars.expected_app[mapped_id_key]=normalized_app

    return expected_map, expected_norms, expected_app


def get_house_type(vendor, data):  # To be reused
    expected_house_type=None

    mapped_type=None
    p_type=gen_params.mls_house_type_key.get(vendor)
    if p_type:
        if '/' in p_type:
            p_type=p_type.split('/')
            mapped_type=data.get(p_type[0])
            if not mapped_type:
                mapped_type=data.get(p_type[1])

            # if housetype == 're':
            #     p_type=p_type[0]
            # else:
            #     p_type=p_type[1]


        else:
            mapped_type=data.get(p_type)

    if vendor == 'wn_crmls' and (mapped_type == 'null' or not mapped_type):
        p_type='property_type'
        mapped_type=data.get(p_type)

    if mapped_type:
        if mapped_type.lower() in genmapping.house_type_map[vendor]['OtherValues']:
            expected_house_type='other'
        elif mapped_type.lower() in genmapping.house_type_map[vendor]['condoValues']:
            expected_house_type='condo'
        elif mapped_type.lower() in genmapping.house_type_map[vendor]['houseValues']:
            expected_house_type='house'
        elif mapped_type.lower() in genmapping.house_type_map[vendor]['multifamilyValues']:
            expected_house_type='multifamily'
        elif mapped_type.lower() in genmapping.house_type_map[vendor]['townhouseValues']:
            expected_house_type='townhouse'
        else:
            expected_house_type=mapped_type

    else:
        expected_house_type=None
    if expected_house_type != 'multifamily':
        housetype='re'
    else:
        housetype='ri'

    return housetype


def set_expected_app(last_records, folder=None, json_file=None, db=None):
    # set expected map
    expected_map={}
    for id_key, val_list in last_records.items():
        for val in val_list:
            if not val.get('data'):
                env_vars.logger.critical('No data found for {}'.format(id_key))
                continue
            vendor, listing_id, housetype=res_by_id_class.get_id_key_details(id_key)

            if vendor == 'sandicor':
                mapped={}
                if folder == 'properties':
                    mapped=sandicor_properties_mapper(val.get('data'))
                elif folder == 'photos':
                    mapped=sandicor_photos_mapper(val.get('data'))
                elif folder == 'openhouse':
                    mapped=sandicor_open_house_mapper(val.get('data'))

                mapped_structure={'data': mapped, 'reali_meta_data': val.get('reali_meta_data')}
                if not expected_map.get(id_key):
                    expected_map[id_key]=[mapped_structure]
                else:
                    expected_map[id_key].append(mapped_structure)

            else:
                if not expected_map.get(id_key):
                    expected_map[id_key]=[val]
                else:
                    expected_map[id_key].append(val)

        if expected_map.get(id_key):
            env_vars.expected_map[id_key]=expected_map[id_key]
        else:
            env_vars.expected_map[id_key]=[{'data': {}, 'reali_meta_data': {}}]

    # set expected app
    expected_app={}
    normalized_app={}
    for mapped_id_key, mapped_val_list in expected_map.items():
        for mapped_val in mapped_val_list:
            if not mapped_val.get('data'):
                continue

        if folder:
            if folder == 'properties':
                normalized_app=val_normalizator.get_mongo_normalized_properties(env_vars, mapped_id_key, norm_val=None,
                                                                                db=db)
            elif folder == 'photos':
                normalized_app=val_normalizator.get_mongo_normalized_photos(env_vars, mapped_id_key, norm_val=None)
            elif folder == 'openhouse':
                normalized_app=val_normalizator.get_mongo_normalized_openhouse(env_vars, mapped_id_key, norm_val=None)

        expected_app[mapped_id_key]=normalized_app
        env_vars.expected_norms[mapped_id_key]=normalized_app

    return expected_app

def set_agent_office_expected(env_vars, content, folder, id_key):
    agent_key=None
    vendor, listing_id, housetype=res_by_id_class.get_id_key_details(id_key)
    agent_id=content['data'].get('listing_agent_id')
    agent_name=content['data'].get('agent_name')
    office_id=content['data'].get('listing_office_id')
    office_name=content['data'].get('office_name')
    if vendor == 'sandicor':
        mls='ca_sdmls'
    else:
        mls=genmapping.mls_market_name.get(vendor)

    # if office_id and office_name:
    #     office_key="mo_id='{}' and mo_full_name='{}'".format(office_id, office_name)
    if office_id:
        office_key="mo_id='{}'".format(office_id)
    else:
        office_key="mo_full_name='{}'".format(office_name)

    if agent_id:
        agent_key="ma_id='{}'".format(agent_id)
    elif agent_name:
        agent_key="ma_name='{}'".format(agent_name)
    # else:
    #     agent_key='ma_name={}'.format(agent_name)

    if agent_key:
        agent_query='select row_to_json(t) from (select * from {}_mls_agents where {} order by mls_update_date) t'.format(
            mls,
            agent_key)

        office_query='select row_to_json(t) from (select * from {}_mls_offices where {} order by mls_update_date) t'.format(
            mls,
            office_key)

        agent_details=postgress_class.get_query_psg(DB_WN, agent_query)
        office_details=postgress_class.get_query_psg(DB_WN, office_query)
        if len(agent_details) > 0:
            agent_name=agent_details[0][0].get('ma_full_name')
            env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['expertInfo.agentName']=agent_name

            env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['expertInfo.agentEmail']= \
                agent_details[0][0].get('ma_email')

            env_vars.expected_by_folder[folder][id_key]['expected_app'][0][
                'expertInfo.agentPhone']=get_first(agent_details[0][0],
                                                   "[ma_preferred_phone,ma_cell_phone,ma_toll_free_phone]")
            env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['expertInfo.agentBRE']= \
                agent_details[0][0].get('ma_license_number')

        if len(office_details) > 0:
            office_name=office_details[0][0].get('mo_full_name')
            env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['expertInfo.officeName']=office_name

            env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['expertInfo.officeEmail']='' \
                if not get_first(office_details[0][0],
                                 "[mo_email,mo_contact_email,mo_broker_email]") else get_first(
                office_details[0][0], "[mo_email,mo_contact_email,mo_broker_email]")
            env_vars.expected_by_folder[folder][id_key]['expected_app'][0][
                'expertInfo.officePhone']='' if not get_first(office_details[0][0],
                                                              "[mo_phone,mo_contact_phone,mo_broker_phone]") else get_first(
                office_details[0][0], "[mo_phone,mo_contact_phone,mo_broker_phone]")

        base_description=env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['description']
        import datetime
        date_time_pattern='%Y-%m-%dT%H:%M:%S'
        mapped_time=content['data'].get('list_date')
        year=str(datetime.datetime.strptime(mapped_time, date_time_pattern).year)
        mls_complience_name='{}, Inc.'.format(genmapping.mls_vendor_type.get(vendor))

        compliance="\n\nListing provided courtesy of {}, {}\n\nProperty Information ©{} {} All Rights reserved".format(
            agent_name, office_name, year, mls_complience_name)
        if not base_description:
            base_description=''
        env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['description']=base_description + compliance


def set_expected_geo(envin, folder, id_key):
    lng=env_vars.expected_by_folder[folder][id_key]['expected_map'][0]['data'].get('lng')
    lat=env_vars.expected_by_folder[folder][id_key]['expected_map'][0]['data'].get('lat')
    # The geopm input data for home junction was stored with  spatial reference system of 4269 (verify with SELECT ST_SRID(geom) FROM hj_neighborhoods LIMIT 1;)
    # Rge SRID used is 4326
    neighborhood_query="select row_to_json(t) from (SELECT * FROM hj_neighborhoods WHERE ST_Contains(geom, ST_Transform(ST_GeometryFromText('POINT({} {})',4326), 4269)) and \"level\"=2) t".format(
        lng, lat)
    neighborhood=postgress_class.get_query_psg(DB_GOLD[envin], neighborhood_query)
    if neighborhood and len(neighborhood) > 0:
        geo_neighborhood=neighborhood[0][0].get('name')
        env_vars.expected_by_folder[folder][id_key]['expected_app'][0]['location.neighborhood']=geo_neighborhood
        env_vars.expected_by_folder[folder][id_key]['expected_norms'][0]['data']['neighborhood']=geo_neighborhood


def json_schema_validator(content, schema_path):
    schema_dir=os.path.dirname(os.path.abspath(schema_path))

    lower_dict={'data': {}}
    for key, val in content.get('data').items():
        lower_dict['data'][key.lower()]=str(val).lower() if val and isinstance(val,
                                                                               str) else val
    with open(schema_path, 'r') as f:
        schema_data=f.read()
        schema=json.loads(schema_data)
        resolver=jsonschema.RefResolver(base_uri='file://' + schema_dir + '/', referrer=schema)

    res=jsonschema.validate(lower_dict.get('data'), schema, resolver=resolver)
    return res


def clear_expected(env_vars):
    env_vars.expected_map={}
    env_vars.expected_norms={}
    env_vars.expected_app={}


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


def get_set_secrerts(envin):
    db_mongo={}
    db_gold={}
    db_wn={}
    try:
        services=['de-extractor', 'de-saver', 'comp-proxy']
        for service_name in services:
            if not env_vars.aws_secrets.get(service_name):
                conf=s3_class.get_secrets_by_service(envin, service_name, params)
                env_vars.aws_secrets[service_name]=conf

        db_gold[envin]=set_postgress_db_from_secret_jdbc('de-saver', db_source='rds')
        db_wn=set_postgress_db_from_secret_jdbc('de-extractor', db_source='wolfnet')
        db_mongo=env_vars.aws_secrets['comp-proxy'].get('mongodb').get('uri')
    except:
        info=sys.exc_info()
        print(info)

    return db_mongo, db_gold, db_wn

@pytest.mark.test_sanity_random_file
def test_get_random_file(envin, aws_new):
    # This test will take random file from S3 and verify its db insertion was done properly
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    folder='properties'
    content=None
    listing_id=None
    id_key=None
    if not env_vars.logger:
        set_environment.set_environment(envin, root_dir, env_vars)
    logger=env_vars.logger
    logger.info('{} env - Verify random data from S3 was loaded to db properly'.format(envin))

    bucket=get_s3_bucket_name(envin)
    if not bucket:
        sys.exit(1)

    status=False
    try:
        res_acc=[]
        act_res=[]
        vendor='wn_sfar'

        collection='areas'
        db_mongo_args=get_db_mongo_args(envin, collection)
        areaid_to_city, city_to_areaid=enrichment_class.get_city_areaid_dicts(db_mongo_args)
        env_vars.areaid_to_city=areaid_to_city
        env_vars.city_to_areaid=city_to_areaid

        # set_environment.set_environment(envin, root_dir, env_vars)
        for vendor in params.vendors2_test:
            file_found=False

            # env_vars.date_folder='2020/1/30'
            prefix=vendor + '/' + folder + '/' + env_vars.date_folder

            s3_files=s3_class.get_time_sorted_s3_files(envin, bucket, prefix, sort_desc=True, aws_new=aws_new)

            # get_last_modified=lambda obj: int(obj.last_modified.strftime('%s'))
            # if len(s3_files) == 0:
            #     print('No files were found at s3 bucket {}. folder_name name  {}'.format(bucket, prefix))
            #     return False
            # sorted_files=sorted(s3_files, key=get_last_modified, reverse=True)

            while not file_found:
                for file_object in s3_files:
                    eliminate=['elad', 'auto']
                    if file_object.size > 2000 and file_object.key and not any(
                            k in file_object.key.lower() for k in eliminate):
                        prefix=file_object.key
                        file_content=s3_class.read_s3_file(envin, bucket, prefix)
                        vendor_scheme_name=genmapping.mls_vendor_type.get(vendor)
                        schema_path=megatron_root_dir + '/gen_config/schema_validator_basic_filter/{}_schema.json'.format(
                            vendor_scheme_name)

                        for content in file_content:
                            housetype=get_house_type(vendor, content.get('data'))
                            if vendor == 'sandicor':
                                id='L_ListingID'
                            else:
                                id='property_id'
                            listing_id=content['data'].get(id)  # To add Error handling
                            id_key='{}/{}/{}'.format(vendor, listing_id, housetype)
                            ref_content={id_key: content}
                            expected_map, expected_norms, expected_app=set_expected(ref_content, folder, db=DB_GOLD)
                            content_norms=expected_norms[id_key][0]
                            try:
                                json_schema_validator(content_norms, schema_path)
                            except jsonschema.exceptions.ValidationError as ve:
                                print("{}\n".format(str(ve)))
                                clear_expected(env_vars)
                                continue
                            lower_dict={'data': {}}

                            schema_path=megatron_root_dir + '/gen_config/schema_validator_mark_filter/{}_schema.json'.format(
                                vendor_scheme_name)

                            content_norms=expected_norms[id_key][0]
                            try:
                                json_schema_validator(content_norms, schema_path)
                            except jsonschema.exceptions.ValidationError as ve:
                                print("{}\n".format(str(ve)))
                                clear_expected(env_vars)
                                continue

                            file_found=True
                            logger.info('S3 File Under Test = {}'.format(file_object.key))

                            if env_vars.expected_by_folder.get(folder):
                                env_vars.expected_by_folder[folder][id_key]={'expected_map': expected_map.get(id_key),
                                                                             'expected_norms': expected_norms.get(
                                                                                 id_key),
                                                                             'expected_app': expected_app.get(id_key)}
                            else:
                                env_vars.expected_by_folder[folder]={id_key: {'expected_map': expected_map.get(id_key),
                                                                              'expected_norms': expected_norms.get(
                                                                                  id_key),
                                                                              'expected_app': expected_app.get(id_key)}}
                            res_acc.append(True)
                            break
                        if file_found:
                            env_vars.used_ids[listing_id]=id_key

                            if content:
                                set_agent_office_expected(env_vars, content, folder, id_key)
                                # set_expected_geo(envin, folder, id_key)
                            break
        if False not in res_acc and len(res_acc) > 0:
            status=True
    except:
        info=sys.exc_info()
        print(info)

    assert status


@pytest.mark.test_sanity_random_file
def test_auto_properties_mapped(envin, aws_new):
    # This test will take random file from S3 and verify its db insertion was done properly
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    folder='properties'
    res_acc=[]
    status=False

    try:

        for id_key, record in env_vars.expected_map.items():
            vendor, listing_id, housetype=res_by_id_class.get_id_key_details(id_key)
            logger=env_vars.logger

            db_table='mapped'
            print('running query on {} table'.format(db_table))
            logger.info('running query on {} table'.format(db_table))
            listings_query=set_query_psg(query_type='properties_listing_ids',
                                         table=DB_GOLD[envin]['collection'][db_table],
                                         list_for_in_query=[listing_id])
            print('quering postgress {} with: {}'.format(envin, [listing_id]))

            psg_resp=postgress_class.get_query_psg(DB_GOLD[envin], listings_query)

            # order properties_by_id query response
            properties_res_by_id, mark_dup=res_by_id_class.get_properties_res_by_id(psg_resp)

            expected_res=env_vars.expected_by_folder.get(folder)

            res=check_results_class.check_res(properties_res_by_id, expected_res, logger, test_type='expected_map',
                                              res_type='data')
            res_acc.append(res)

            if False not in res_acc and len(res_acc) > 0:
                status=True
    except:
        info=sys.exc_info()
        print(info)

    assert status


@pytest.mark.test_sanity_random_file
def test_auto_properties_normalized(envin, aws_new):
    # This test will take random file from S3 and verify its db insertion was done properly
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    folder='properties'
    res_acc=[]
    status=False

    try:
        logger=env_vars.logger

        for id_key, record in env_vars.expected_norms.items():
            vendor, listing_id, housetype=res_by_id_class.get_id_key_details(id_key)

            db_table='normalized'
            print('running query on {} table'.format(db_table))
            logger.info('running query on {} table'.format(db_table))
            listings_query=set_query_psg(query_type='normalized_data_query',
                                         table=DB_GOLD[envin]['collection'][db_table],
                                         list_for_in_query=[listing_id])
            print('quering postgress {} with: {}'.format(envin, [listing_id]))

            psg_resp=postgress_class.get_query_psg(DB_GOLD[envin], listings_query)

            # order properties_by_id query response
            properties_res_by_id, mark_dup=res_by_id_class.get_properties_res_by_id(psg_resp)

            expected_res=env_vars.expected_by_folder.get(folder)

            res=check_results_class.check_res(properties_res_by_id, expected_res, logger,
                                              test_type='expected_norms',
                                              res_type='data')
            res_acc.append(res)

        if False not in res_acc and len(res_acc) > 0:
            status=True
    except:
        info=sys.exc_info()
        print(info)

    assert status


@pytest.mark.test_sanity_random_file
def test_auto_properties_app(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    status=False
    res_acc=[]
    test_type={'filetype': 'new'}
    # env_vars.test_enriched=False
    # env_vars.test_agents=False
    try:

        if not env_vars.logger:
            set_environment.set_environment(envin, root_dir, env_vars)
        logger=env_vars.logger

        folder='properties'

        if env_vars.expected_by_folder.get(folder):
            expected_res=env_vars.expected_by_folder.get(folder)
            app_res_by_id=env_vars.app_res_by_id
            used_ids=list(env_vars.used_ids.keys())
            db_mongo_args=get_db_mongo_args(envin, 'listings')
            properties_res_by_id=res_by_id_class.get_app_res_by_id(envin, used_ids, app_res_by_id, db_mongo_args)
            env_vars.app_res_by_id=properties_res_by_id
            res=check_results_class.check_mongo_res(properties_res_by_id, expected_res, logger,
                                                    test_type='expected_app',
                                                    res_type='data')
            res_acc.append(res)

        if False not in res_acc and len(res_acc) > 0:
            status=True
        else:
            status=False
    except:
        info=sys.exc_info()
        print(info)

    assert status


@pytest.mark.test_sanity_random_file
def test_auto_properties_app_price_prediction(envin, aws_new):
    if str(aws_new).lower() != 'true':
        aws_new=False
    params.aws_new_account=aws_new
    print('\n\n*** Working on AWS new account = {} ***\n'.format(aws_new))
    status=False
    res_acc=[]
    test_type={'filetype': 'new'}
    # env_vars.test_enriched=False
    # env_vars.test_agents=False
    try:

        if not env_vars.logger:
            set_environment.set_environment(envin, root_dir, env_vars)
        logger=env_vars.logger

        folder='properties'
        DB_MONGO, DB_GOLD, DB_WN=get_set_secrerts(envin)

        if env_vars.expected_by_folder.get(folder):
            used_ids=list(env_vars.used_ids.keys())
            properties_res_by_id=res_by_id_class.get_app_price_prediction_by_id(envin, used_ids, DB_MONGO)
            env_vars.app_prediction_res_by_id=properties_res_by_id
            expected_res=env_vars.expected_by_folder.get(folder)
            res=check_results_class.check_mongo_res_price_prediction(properties_res_by_id, expected_res, logger,
                                                                     test_type='expected_app',
                                                                     res_type='data')
            res_acc.append(res)

        if False not in res_acc and len(res_acc) > 0:
            status=True
        else:
            status=False
    except:
        info=sys.exc_info()
        print(info)

    assert status
