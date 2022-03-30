import datetime
from collections import OrderedDict

delta_minutes = 10
delta_days = 1
delta_hours = 5
delta_months = 6 * 30 * 24  # actual hours
# timestamp = str(datetime.datetime.today().date() - datetime.timedelta(days=delta_days))
# delta_days = 47 * 24 # actual hours
# delta_hours = 0

# timestamp = str(datetime.datetime.today().date() - datetime.timedelta(days=delta_days))

timestamp_minutes = str(datetime.datetime.now() - datetime.timedelta(minutes=delta_minutes))
timestamp_hours = str(datetime.datetime.now() - datetime.timedelta(hours=delta_hours))
timestamp_months = str(datetime.datetime.now() - datetime.timedelta(hours=delta_months))
timestamp_days = str(datetime.datetime.now() - datetime.timedelta(hours=delta_days))

# timestamp_months = timestamp_days
mongo_coll = 'listings'

vendors2_test = ['wn_crmls', 'wn_bay_east', 'wn_bareis', 'wn_fresno', 'wn_mls_listings', 'wn_metrolist',
                 'wn_santa_barbara', 'wn_sfar']

vendors_map = OrderedDict({
    'wn_bareis': 'ca_bareis',
    'bareis': 'ca_bareis',
    'wn_sfar': 'ca_sfar',
    'sfar_mls': 'ca_sfar',
    'wn_santa_barbara': 'ca_sbaor',
    'sbaor': 'ca_sbaor',
    'wn_crmls': 'ca_mrmls',
    'crmls': 'ca_mrmls',
    'wn_metrolist': 'ca_metrolist',
    'metrolist': 'ca_metrolist',
    'wn_fresno': 'ca_fmls',
    'fmls': 'ca_fmls',
    'wn_bay_east': 'ca_bear',
    'bayeast': 'ca_bear',
    'wn_mls_listings': 'ca_mlslistings',
    'mls_listings': 'ca_mlslistings'
})

common_filters_6_months_back = """ 
last_update_date >'{}' 
and last_update_date < '{}' :: timestamp 
and city not ilike '%out of area%' 
and city not ilike '%outside area%' 
and city not ilike '%other%' 
and display_address is not null 
and display_address <> '' 
and street_number is not null 
and street_number not in ('',' ','0') 
and listing_price > 30000::money 
and state='CA'
and city !~ '.*[0-9].*'
and city is not null
and length(city)>3 
and lower(office_name) <> 'reali'
and (is_residential_lease <> 'Y' or is_residential_lease is null)
""".format(timestamp_months, timestamp_hours)

# common_filters="""
# last_update_date < '{}' :: timestamp
# and city not ilike '%out of area%'
# and city not ilike '%outside area%'
# and city not ilike '%other%'
# and display_address is not null
# and display_address <> ''
# and street_number is not null
# and street_number not in ('',' ','0')
# and listing_price > 30000::money
# and state='CA'
# and (city !~ '.*[0-9].*' or lower(city)='29 palms')
# and city is not null
# and length(city)>3
# and lower(office_name) <> 'reali'
# and (is_residential_lease <> 'Y' or is_residential_lease is null)
# """.format(timestamp_minutes)

common_filters = """ 
mls_update_date > '2017-01-01 00:00:00.0' :: timestamp 
and city not ilike '%out of area%' 
and city not ilike '%outside area%' 
and city not ilike '%other%' 
and display_address is not null 
and display_address <> '' 
and street_number is not null 
and street_number not in ('',' ','0') 
and listing_price > 30000::money 
and state='CA'
and (city !~ '.*[0-9].*' or lower(city)='29 palms')
and city is not null
and length(city)>3 
and lower(office_name) <> 'reali'
and (is_residential_lease <> 'Y' or is_residential_lease is null)
""".format(timestamp_minutes)


def common_joind_filters_short(table):
    return """ 
p.last_update_date < '{0}' :: timestamp
and p.last_update_date > '2017-01-01 00:00:00' :: timestamp
and p.display_address is not null 
and p.display_address <> '' 
and p.street_number is not null 
and p.street_number not in ('',' ','0') 
and p.listing_price >= 30000::money 
and p.state='CA'
and lower(p.office_name) <> 'reali'
and (p.is_residential_lease <> 'Y' or p.is_residential_lease is null)
""".format(timestamp_hours)


vendors_queries = {
    'wn_crmls':
        "and lower(property_type) in ('residential income','residential') "
        "and ("
        "(lower(property_sub_type) in ('stock cooperative','townhouse','studio', 'condominium', 'loft', 'single family residence', 'manufactured on land', 'manufactured home') and (total_bedrooms>0 or total_bathrooms>0) "
        "or lower(property_sub_type) in ('duplex', 'multi family', 'triplex', 'quadruplex')"
        ")"
        "or ("
        "("
        "lower(property_sub_type) is null "
        "and (lower(property_type) in ('residential income') or (lower(property_type) in ('residential') and (total_bedrooms>0 or total_bathrooms>0))) "
        ")"
        ")"
        ") and listing_status in ('Active','Pending','Active Under Contract') "
        "and lower(originatingsystemname) not in ('mlsl','bmls') "
        "and listing_agent_id not ilike '%sand%'",
    'wn_crmls_old_status':
        "and lower(property_type) in ('residential income','residential') "
        "and ("
        "(lower(property_sub_type) in ('stock cooperative','townhouse','studio', 'condominium', 'loft', 'single family residence', 'manufactured on land', 'manufactured home') and (total_bedrooms>0 or total_bathrooms>0) "
        "or lower(property_sub_type) in ('duplex', 'multi family', 'triplex', 'quadruplex')"
        ")"
        "or ("
        "("
        "lower(property_sub_type) is null "
        "and (lower(property_type) in ('residential income') or (lower(property_type) in ('residential') and (total_bedrooms>0 or total_bathrooms>0))) "
        ")"
        ")"
        ") and listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract') "
        "and lower(originatingsystemname) not in ('mlsl','bmls') "
        "and listing_agent_id not ilike '%sand%'",

    'wn_bareis':
        "and ("
        "lower(property_type) in ('multi unit 2-4') "
        "or (lower(property_type) in ('condo/coop', 'single family residence', 'townhouse', 'floating home') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Contingent - Release', 'Contingent-Show', 'Contingent - No Show', 'Pending')",

    'wn_sfar':
        "and (lower(property_sub_type) in ('2 units', '3 units', '4 units') or (lower(property_sub_type) in ('single-family homes','condominium', 'loft condominium', 'tenancy in common', 'stock cooperative') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active','Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Act Cont No Show', 'Act Cont Short Sale')",

    'wn_sfar_old_status':
        "and (lower(property_sub_type) in ('2 units', '3 units', '4 units') or (lower(property_sub_type) in ('single-family homes','condominium', 'loft condominium', 'tenancy in common', 'stock cooperative') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract')",

    'wn_santa_barbara':
        "and (property_type in ('Multi Family') or (property_type in ('Home/Estate', 'Manufactured Housing', 'Ranch/Farm', 'Condo/Co-Op') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Pending')",

    'wn_metrolist':
        "and (property_sub_type in ('Duplex', '2 Houses on Lot', 'Halfplex', '5 or More Units', 'Fourplex', 'Triplex', '3+ Houses on Lot') or (property_sub_type in ('1 House on Lot', 'Residential Lot', 'Residential Acreage','Condo') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active','Active Short Sale','Active Rel. Clause','Active Short Cont.','Active Court Cont.','Active Court Appr.','Pending Bring Backup','Pending Short LAppvl','Pending','Pending Sale') "
        "and lower(listing_status) <> 'off market'",

    'wn_fresno':
        "and (lower(property_sub_type) in ('quadruplex', 'duplex', 'triplex','quadruplex duplex','triplex duplex','triplex duplex quadruplex') "
        "or (lower(property_sub_type) in ('condominium pud single family residence','pud single family residence', 'condominium', 'condominium single family residence', 'condominium single family residence pud', 'single family residence condominium pud','single family residence pud condominium','single family residence condominium', 'agricultural single family residence','condominium pud', 'single family residence', 'single family residence pud', 'manufactured home', 'single family residence manufactured home', 'manufactured home single family residence', 'pud condominium single family residence','pud single family residence condominium','single family residence agricultural') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Coming Soon','Active','Pending AcceptBackup','Pending')",

    'wn_bay_east':
        "and (lower(property_type) in ('residential income') or (lower(property_type) in ('residential') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and lower(property_sub_type) in ('condo', 'loft', 'flats', 'detached', 'duet', 'patio home/villa', 'floating homes', 'duplex', 'triplex', 'fourplex', '2houses-1lot', 'townhouse') "
        "and listing_status in ('Active', 'Back on Market', 'New', 'Price Change', 'Active-REO', 'Active-Short Sale', 'Back on Market-REO', 'Back on Market-Short Sale', 'New-REO', 'New-Short Sale', 'Price Change-REO', 'Price Change-Short Sale', 'Pending', 'Pending CourtConfirmation', 'Pending Show for Backups', 'Pending Subj LenderApprov', 'Pending-REO', 'Pending Show Backups-REO', 'Pending Show Backup-Short', 'Active - Contingent') "
        "and lower(originatingsystemname) not in ('mlslisitngs','mlslistings datashare')",

    'wn_mls_listings':
        "and (lower(property_type) in ('multi family') "
        "or (lower(property_type) in ('single family', 'townhouse', 'condominium') and (total_bedrooms>0 or total_bathrooms>0)))"
        "and ((listing_status in ('Active', 'Contingent', 'Pending (Do Not Show)') or (listing_status in ('Pending (Do Not Show)') and (display_address is null or display_address = ''))))"
}

vendors_join_filters_short = {
    'wn_crmls':
        common_joind_filters_short('ca_mrmls_properties') + vendors_queries['wn_crmls'],

    'wn_bareis':
        common_joind_filters_short('ca_bareis_properties') + vendors_queries['wn_bareis'],

    'wn_sfar':
        common_joind_filters_short('ca_sfar_properties') + vendors_queries['wn_sfar'],

    'wn_santa_barbara':
        common_joind_filters_short('ca_sbaor_properties') + vendors_queries['wn_santa_barbara'],

    'wn_metrolist':
        common_joind_filters_short('ca_metrolist_properties') + vendors_queries['wn_metrolist'],

    'wn_fresno':
        common_joind_filters_short('ca_fmls_properties') + vendors_queries['wn_fresno'],

    'wn_bay_east':
        common_joind_filters_short('ca_bear_properties') + vendors_queries['wn_bay_east'],

    'wn_mls_listings':
        common_joind_filters_short('ca_mlslistings_properties') + vendors_queries['wn_mls_listings'],
}


def common_joind_filters_Back(table):
    return """ 
{0}.last_update_date >'{1}' 
and {0}.last_update_date < '{2}' :: timestamp
and {0}.display_address is not null 
and {0}.display_address <> '' 
and {0}.street_number is not null 
and {0}.street_number not in ('',' ','0') 
and {0}.listing_price >= 30000::money 
and {0}.state='CA'
and lower({0}.office_name) <> 'reali'
and ({0}.is_residential_lease <> 'Y' or {0}.is_residential_lease is null)
""".format(table, timestamp_months, timestamp_hours)


def common_joind_filters_back(table):
    return """ 
properties.mls_update_date > '2017-01-01 00:00:00.0' :: timestamp 
and {0}.display_address is not null 
and {0}.display_address <> '' 
and {0}.street_number is not null 
and {0}.street_number not in ('',' ','0') 
and {0}.listing_price >= 30000::money 
and {0}.state='CA'
and lower({0}.office_name) <> 'reali'
and ({0}.is_residential_lease <> 'Y' or {0}.is_residential_lease is null)
""".format(table, timestamp_months, timestamp_hours)


def common_joind_filters(table='properties'):
    return """ 
properties.mls_update_date > '2017-01-01 00:00:00.0' :: timestamp 
and properties.display_address is not null 
and properties.display_address <> '' 
and properties.street_number is not null 
and properties.street_number not in ('',' ','0') 
and properties.listing_price >= 30000::money 
and properties.state='CA'
and lower(properties.office_name) <> 'reali'
and (properties.is_residential_lease <> 'Y' or properties.is_residential_lease is null)
""".format(table, timestamp_months, timestamp_hours)


conn_geo = None
conn_mongo = None
DB_PSG_BB = {
    'collection': 'mls_properties',
    'dev': {
        'host': 'dev-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'mls_properties'
    },
    'qa': {
        'host': 'qa-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'mls_properties'
    },
    'staging': {
        'host': 'staging-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'mls_properties'
    },
    'prod': {
        'host': 'production-mls-ds.ccnlwblpbzoe.us-west-2.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'mls_properties'
    }
}

vendors_with_house_type = ['sandicor', 'wn_metrolist', 'wn_fresno', 'wn_santa_barbara', 'wn_sfar', 'wn_bareis',
                           'wn_bay_east', 'wn_mls_listings']

vendor_active_list = {
    'wn_crmls':
        ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New', 'Pending', 'Pending Sale',
         'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show', 'Active Under Contract'),

    'wn_bareis': ('Active', 'Contingent - Release', 'Contingent-Show', 'Contingent - No Show', 'Pending'),

    'wn_sfar':
        ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New', 'Pending', 'Pending Sale',
         'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show', 'Active Under Contract'),

    'wn_santa_barbara':
        ('Active', 'Pending'),

    'wn_metrolist':
        ('Active', 'Active Short Sale', 'Active Rel. Clause', 'Active Short Cont.', 'Active Court Cont.',
         'Active Court Appr.', 'Pending Bring Backup', 'Pending Short LAppvl', 'Pending', 'Pending Sale'),

    'wn_fresno':
        ('Coming Soon', 'Active', 'Pending AcceptBackup', 'Pending'),

    'wn_bay_east':
        ('Active', 'Back on Market', 'New', 'Price Change', 'Active-REO', 'Active-Short Sale', 'Back on Market-REO',
         'Back on Market-Short Sale', 'New-REO', 'New-Short Sale', 'Price Change-REO', 'Price Change-Short Sale',
         'Pending', 'Pending CourtConfirmation', 'Pending Show for Backups', 'Pending Subj LenderApprov', 'Pending-REO',
         'Pending Show Backups-REO', 'Pending Show Backup-Short', 'Active - Contingent'),

    'wn_mls_listings':
        ('Active', 'Contingent', 'Pending (Do Not Show)', 'Pending (Do Not Show)')
}

vendor_active_query = {
    'wn_crmls':
        "listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract')",

    'wn_bareis':
        "listing_status in ('Active', 'Contingent - Release', 'Contingent-Show', 'Contingent - No Show', 'Pending')",

    'wn_sfar':
        "listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract')",

    'wn_santa_barbara':
        "listing_status in ('Active', 'Pending')",

    'wn_metrolist':
        "listing_status in ('Active','Active Short Sale','Active Rel. Clause','Active Short Cont.','Active Court Cont.','Active Court Appr.','Pending Bring Backup','Pending Short LAppvl','Pending','Pending Sale')",

    'wn_fresno':
        "listing_status in ('Coming Soon','Active','Pending AcceptBackup','Pending')",

    'wn_bay_east':
        "listing_status in ('Active', 'Back on Market', 'New', 'Price Change', 'Active-REO', 'Active-Short Sale', 'Back on Market-REO', 'Back on Market-Short Sale', 'New-REO', 'New-Short Sale', 'Price Change-REO', 'Price Change-Short Sale', 'Pending', 'Pending CourtConfirmation', 'Pending Show for Backups', 'Pending Subj LenderApprov', 'Pending-REO', 'Pending Show Backups-REO', 'Pending Show Backup-Short', 'Active - Contingent')",

    'wn_mls_listings':
        "((listing_status in ('Active', 'Contingent', 'Pending (Do Not Show)') or (listing_status in ('Pending (Do Not Show)') and (display_address is null or display_address = ''))))"
}

vendors_queries = {
    'wn_crmls':
        "and lower(property_type) in ('residential income','residential') "
        "and ("
        "(lower(property_sub_type) in ('stock cooperative','townhouse','studio', 'condominium', 'loft', 'single family residence', 'manufactured on land', 'manufactured home') and (total_bedrooms>0 or total_bathrooms>0) "
        "or lower(property_sub_type) in ('duplex', 'multi family', 'triplex', 'quadruplex')"
        ")"
        "or ("
        "("
        "lower(property_sub_type) is null "
        "and (lower(property_type) in ('residential income') or (lower(property_type) in ('residential') and (total_bedrooms>0 or total_bathrooms>0))) "
        ")"
        ")"
        ") and listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract') "
        "and lower(originatingsystemname) not in ('mlsl','bmls') "
        "and listing_agent_id not ilike '%sand%'",

    'wn_bareis':
        "and ("
        "lower(property_type) in ('multi unit 2-4') "
        "or (lower(property_type) in ('condo/coop', 'single family residence', 'townhouse', 'floating home') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Contingent - Release', 'Contingent-Show', 'Contingent - No Show', 'Pending')",

    'wn_sfar':
        "and (lower(property_sub_type) in ('2 units', '3 units', '4 units') or (lower(property_sub_type) in ('single-family homes','condominium', 'loft condominium', 'tenancy in common', 'stock cooperative') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Active Court Appr.', 'Active Court Cont.', 'Active Rel. Clause', 'New','Pending', 'Pending Sale', 'Act Cont Probate', 'Act Cont Rel Clause', 'Act Cont Show','Active Under Contract')",

    'wn_santa_barbara':
        "and (property_type in ('Multi Family') or (property_type in ('Home/Estate', 'Manufactured Housing', 'Ranch/Farm', 'Condo/Co-Op') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active', 'Pending')",

    'wn_metrolist':
        "and (property_sub_type in ('Duplex', '2 Houses on Lot', 'Halfplex', '5 or More Units', 'Fourplex', 'Triplex', '3+ Houses on Lot') or (property_sub_type in ('1 House on Lot', 'Residential Lot', 'Residential Acreage','Condo') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Active','Active Short Sale','Active Rel. Clause','Active Short Cont.','Active Court Cont.','Active Court Appr.','Pending Bring Backup','Pending Short LAppvl','Pending','Pending Sale') "
        "and lower(listing_status) <> 'off market'",

    'wn_fresno':
        "and (lower(property_sub_type) in ('quadruplex', 'duplex', 'triplex','quadruplex duplex','triplex duplex','triplex duplex quadruplex') "
        "or (lower(property_sub_type) in ('condominium pud single family residence','pud single family residence', 'condominium', 'condominium single family residence', 'condominium single family residence pud', 'single family residence condominium pud','single family residence pud condominium','single family residence condominium', 'agricultural single family residence','condominium pud', 'single family residence', 'single family residence pud', 'manufactured home', 'single family residence manufactured home', 'manufactured home single family residence', 'pud condominium single family residence','pud single family residence condominium','single family residence agricultural') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and listing_status in ('Coming Soon','Active','Pending AcceptBackup','Pending')",

    'wn_bay_east':
        "and (lower(property_type) in ('residential income') or (lower(property_type) in ('residential') and (total_bedrooms>0 or total_bathrooms>0))) "
        "and lower(property_sub_type) in ('condo', 'loft', 'flats', 'detached', 'duet', 'patio home/villa', 'floating homes', 'duplex', 'triplex', 'fourplex', '2houses-1lot', 'townhouse') "
        "and listing_status in ('Active', 'Back on Market', 'New', 'Price Change', 'Active-REO', 'Active-Short Sale', 'Back on Market-REO', 'Back on Market-Short Sale', 'New-REO', 'New-Short Sale', 'Price Change-REO', 'Price Change-Short Sale', 'Pending', 'Pending CourtConfirmation', 'Pending Show for Backups', 'Pending Subj LenderApprov', 'Pending-REO', 'Pending Show Backups-REO', 'Pending Show Backup-Short', 'Active - Contingent') "
        "and lower(originatingsystemname) not in ('mlslisitngs','mlslistings datashare')",

    'wn_mls_listings':
        "and (lower(property_type) in ('multi family') "
        "or (lower(property_type) in ('single family', 'townhouse', 'condominium') and (total_bedrooms>0 or total_bathrooms>0)))"
        "and ((listing_status in ('Active', 'Contingent', 'Pending (Do Not Show)') or (listing_status in ('Pending (Do Not Show)') and (display_address is null or display_address = ''))))"
}

vendors_filters = {
    'wn_crmls':
        common_filters + vendors_queries['wn_crmls'],

    'wn_bareis':
        common_filters + vendors_queries['wn_bareis'],
    'wn_sfar':
        common_filters + vendors_queries['wn_sfar'],

    'wn_santa_barbara':
        common_filters + vendors_queries['wn_santa_barbara'],

    'wn_metrolist':
        common_filters + vendors_queries['wn_metrolist'],

    'wn_fresno':
        common_filters + vendors_queries['wn_fresno'],

    'wn_bay_east':
        common_filters + vendors_queries['wn_bay_east'],

    'wn_mls_listings':
        common_filters + vendors_queries['wn_mls_listings']
}

vendors_join_filters_back = {
    'wn_crmls':
        common_joind_filters('ca_mrmls_properties') + vendors_queries['wn_crmls'],

    'wn_bareis':
        common_joind_filters('ca_bareis_properties') + vendors_queries['wn_bareis'],

    'wn_sfar':
        common_joind_filters('ca_sfar_properties') + vendors_queries['wn_sfar'],

    'wn_santa_barbara':
        common_joind_filters('ca_sbaor_properties') + vendors_queries['wn_santa_barbara'],

    'wn_metrolist':
        common_joind_filters('ca_metrolist_properties') + vendors_queries['wn_metrolist'],

    'wn_fresno':
        common_joind_filters('ca_fmls_properties') + vendors_queries['wn_fresno'],

    'wn_bay_east':
        common_joind_filters('ca_bear_properties') + vendors_queries['wn_bay_east'],

    'wn_mls_listings':
        common_joind_filters('ca_mlslistings_properties') + vendors_queries['wn_mls_listings'],
}

vendors_join_filters = {
    'wn_crmls':
        common_joind_filters() + vendors_queries['wn_crmls'],

    'wn_bareis':
        common_joind_filters() + vendors_queries['wn_bareis'],

    'wn_sfar':
        common_joind_filters() + vendors_queries['wn_sfar'],

    'wn_santa_barbara':
        common_joind_filters() + vendors_queries['wn_santa_barbara'],

    'wn_metrolist':
        common_joind_filters() + vendors_queries['wn_metrolist'],

    'wn_fresno':
        common_joind_filters() + vendors_queries['wn_fresno'],

    'wn_bay_east':
        common_joind_filters() + vendors_queries['wn_bay_east'],

    'wn_mls_listings':
        common_joind_filters() + vendors_queries['wn_mls_listings'],
}

mls_vendor_type = {
    'sandicor': 'SANDICOR',
    'wn_crmls': 'CRMLS',
    'crmls': 'CRMLS',
    'wn_sfar': 'SFAR_MLS',
    'sfar': 'SFAR_MLS',
    'wn_bareis': 'BAREIS',
    'bareis': 'BAREIS',
    'wn_metrolist': 'METROLIST',
    'metrolist': 'METROLIST',
    'wn_fresno': 'FMLS',
    'fmls': 'FMLS',
    'wn_santa_barbara': 'SBAOR',
    'sbaor': 'SBAOR',
    'wn_bay_east': 'BAYEAST',
    'bayeast': 'BAYEAST',
    'wn_mls_listings': 'MLS_LISTINGS',
    'mls_listings': 'MLS_LISTINGS'
}

mls_vendor_to_wn = {
    'sandicor': 'wn_sdmls',
    'crmls': 'wn_crmls',
    'sfar_mls': 'wn_sfar',
    'bareis': 'wn_bareis',
    'metrolist': 'wn_metrolist',
    'fmls': 'wn_fresno',
    'sbaor': 'wn_santa_barbara',
    'bayeast': 'wn_bay_east',
    'mls_listings': 'wn_mls_listings'
}


def get_query(test_type, vendor, ids={}):
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    query_test_types = {
        'properties':
            "select row_to_json(t) from (select p.property_id, city from {0}_properties p inner join {0}_property_attributes pa "
            "on p.property_id = pa.property_id where {1}) t"
                .format(vendors_map[vendor], vendors_join_filters_short[vendor]),
        'properties_24':
            "select row_to_json(t) from (select p.property_id, city from {0}_properties p inner join {0}_property_attributes pa "
            "on p.property_id = pa.property_id where {1} and p.mls_update_date > '{2}' :: timestamp) t"
                .format(vendors_map[vendor], vendors_join_filters_short[vendor],
                        str(datetime.datetime.now() - datetime.timedelta(hours=24))),
        'properties_':
            "select row_to_json(t) from(select property_id from {}.{}_properties where {}) t".format(
                DB_WN['collection'], vendors_map[vendor],
                vendors_filters[vendor]),
        'no_private_remarks_filter': "select row_to_json(t) from(select properties.property_id "
                                     "from {0}_properties properties left join {0}_property_attributes pa "
                                     "on properties.property_id = pa.property_id where "
                                     "pa.additional_remarks is null and {1}) t".format(vendors_map[vendor],
                                                                                       vendors_join_filters[vendor]),
        'private_remarks_filter': "select row_to_json(t) from(select properties.property_id "
                                  "from {0}_properties properties left join {0}_property_attributes pa "
                                  "on properties.property_id = pa.property_id where "
                                  "pa.additional_remarks is not null and {1}) t".format(vendors_map[vendor],
                                                                                        vendors_join_filters[vendor]),
        'private_remarks_filter_all': "select row_to_json(t) from(select properties.property_id "
                                      "from {0}_properties properties left join {0}_property_attributes pa "
                                      "on properties.property_id = pa.property_id where "
                                      "{1}) t".format(vendors_map[vendor],
                                                      vendors_join_filters[vendor]),
        'private_remarks': "select row_to_json(t) from(select ca.property_id "
                           "from {0}_properties ca left join {0}_property_attributes pa "
                           "on ca.property_id = pa.property_id where ca.listing_status ='Active' "
                           "and pa.additional_remarks is not null) t".format(vendors_map[vendor]),
        'private_remarks_all': "select row_to_json(t) from(select ca.property_id "
                               "from {0}_properties ca left join {0}_property_attributes pa "
                               "on ca.property_id = pa.property_id where ca.listing_status ='Active') t".format(
            vendors_map[vendor]),
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

        'photos': "select row_to_json(t) from(select photos.property_id, count(*) from {0}_photos photos inner join {0}_properties properties on photos.property_id = properties.property_id "
                  "where {1} and photos.property_id in({2}) group by photos.property_id) t".format(vendors_map[vendor],
                                                                                                   vendors_join_filters[
                                                                                                       vendor],
                                                                                                   ids.get(vendor)),
        'open_house': "select row_to_json(t) from(select distinct mlnumber,fromdate, todate, fromtime, totime, listprice, status "
                      "from {0}.{1}_open_house open_house inner join {0}.{1}_properties properties  on open_house.mlnumber = properties.display_mlsnumber"
                      " where open_house.todate>'{2}' "
                      "and {3} order by fromdate) t".format(
            DB_WN['collection'], vendors_map[vendor], yesterday, vendors_join_filters[vendor]),
        'open_house_1': "select row_to_json(res) from(select t.mlnumber, ARRAY_AGG (row_to_json(t)) from(select distinct mlnumber,fromdate, todate, fromtime, totime, listprice, status "
                        "from {0}.{1}_open_house inner join {0}.{1}_properties on {0}.{1}_open_house.mlnumber = {0}.{1}_properties.display_mlsnumber"
                        " where {0}.{1}_open_house.fromdate>'{2}' "
                        "and {3}) t "
                        "group by mlnumber) res".format(
            DB_WN['collection'], vendors_map[vendor], yesterday, vendors_join_filters[vendor]),
        'duplication': "SELECT property_id, mls, hash_string, is_primary "
                       "FROM public.duplicate "
                       "where not is_primary",
        'white_list':
            "SELECT input_city, real_city "
            "FROM public.cities_dict where is_enabled",
    }
    return query_test_types.get(test_type)


DB_WN = {
    'host': 'wolfnet-proxy.reali.com',
    'port': '6432',
    'dbname': 'wolfnet_data_services',
    'user': 'reali',
    'password': '$5fr59G9',
    'collection': 'data65e7d697'
}

DB_PSG_DUP = {
    'dev': {
        'host': 'dev-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'duplicate'
    },
    'qa': {
        'host': 'qa-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'duplicate'
    },
    'staging': {
        'host': 'staging-mls-ds.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'duplicate'
    },
    'prod': {
        'host': 'production-mls-ds.ccnlwblpbzoe.us-west-2.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'mlsds',
        'user': 'reali',
        'password': 'bumblebee2019',
        'collection': 'duplicate'
    }
}

DB_GEO = {
    'dev': {
        'host': 'geolocation.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'geolocation',
        'user': 'reali',
        'password': 'addressloc',
        'collection': 'geolocation'
    },
    'qa': {
        'host': 'geolocation.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'geolocation',
        'user': 'reali',
        'password': 'addressloc',
        'collection': 'geolocation'
    },
    'staging': {
        'host': 'geolocation.cusy8esu9ul6.us-east-1.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'geolocation',
        'user': 'reali',
        'password': 'addressloc',
        'collection': 'geolocation'
    },
    'prod': {
        'host': 'geolocation.ccnlwblpbzoe.us-west-2.rds.amazonaws.com',
        'port': '5432',
        'dbname': 'geolocation',
        'user': 'reali',
        'password': 'addressloc',
        'collection': 'geolocation'
    }
}

mongo_db = {
    'user': 'RealiDB',
    'password': 'g9CBgF0GYwdSTwa6',
    'comps': {
        'dbname': {
            'dev': 'realiDB',
            'qa': 'realiDB',
            'staging': 'realidb-staging',
            'prod': 'realidb-production'},
        'cluster': {
            'dev': 'reali-dev-shard-00-00-kziwr.mongodb.net:27017, reali-dev-shard-00-01-kziwr.mongodb.net:27017, reali-dev-shard-00-02-kziwr.mongodb.net:27017',
            'qa': 'qa-shard-00-00-kziwr.mongodb.net:27017,qa-shard-00-01-kziwr.mongodb.net:27017,qa-shard-00-02-kziwr.mongodb.net:27017',
            'staging': 'realidb-staging-shard-00-00-kziwr.mongodb.net:27017,realidb-staging-shard-00-01-kziwr.mongodb.net:27017, realidb-staging-shard-00-02-kziwr.mongodb.net:27017',
            'prod': 'echo-prod-shard-00-00-kziwr.mongodb.net:27017,echo-prod-shard-00-01-kziwr.mongodb.net:27017,echo-prod-shard-00-02-kziwr.mongodb.net:27017'},
        'auth': {
            'qa': 'ssl=true&ssl_cert_reqs={}&replicaSet=qa-shard-0&authSource=admin'.format('CERT_NONE'),
            'dev': 'ssl=true&ssl_cert_reqs={}&replicaSet=reali-dev-shard-0&authSource=admin'.format('CERT_NONE'),
            'staging': 'ssl=true&ssl_cert_reqs={}&replicaSet=realidb-staging-shard-0&authSource=admin'.format(
                'CERT_NONE'),
            'prod': 'ssl=true&ssl_cert_reqs={}&replicaSet=echo-prod-shard-0&authSource=admin&authMechanism=SCRAM-SHA-1'.format(
                'CERT_NONE')
        }
    },
    'Properties': {
        'dbname': {
            'dev': 'editorialTransformerDB',
            'qa': 'editorialTransformerDB',
            'staging': 'editorialTransformerDB',
            'prod': 'editorialTransformerDB'},
        'cluster': {
            'dev': 'dev-data-shard-00-00-kziwr.mongodb.net:27017, dev-data-shard-00-01-kziwr.mongodb.net:27017, dev-data-shard-00-02-kziwr.mongodb.net:27017',
            'qa': 'qa-shard-00-00-kziwr.mongodb.net:27017,qa-shard-00-01-kziwr.mongodb.net:27017,qa-shard-00-02-kziwr.mongodb.net:27017',
            'staging': 'realidb-staging-shard-00-00-kziwr.mongodb.net:27017, realidb-staging-shard-00-01-kziwr.mongodb.net:27017, realidb-staging-shard-00-02-kziwr.mongodb.net:27017',
            'prod': 'prod-editorial-shard-00-00-kziwr.mongodb.net:27017,prod-editorial-shard-00-01-kziwr.mongodb.net:27017,prod-editorial-shard-00-02-kziwr.mongodb.net:27017'},
        'auth': {
            'dev': 'ssl=true&ssl_cert_reqs={}&replicaSet=dev-data-shard-0&authSource=admin'.format('CERT_NONE'),
            'qa': 'ssl=true&ssl_cert_reqs={}&replicaSet=qa-shard-0&authSource=admin'.format('CERT_NONE'),
            'staging': 'ssl=true&ssl_cert_reqs={}&replicaSet=realidb-staging-shard-0&authSource=admin'.format(
                'CERT_NONE'),
            'prod': 'ssl=true&ssl_cert_reqs={}&replicaSet=prod-editorial-shard-0&authSource=admin&authMechanism=SCRAM-SHA-1'.format(
                'CERT_NONE')
        }
    }
}

mongo_db_ = {
    'user': 'RealiDB',
    'password': 'g9CBgF0GYwdSTwa6',
    'comps': {
        'dbname': {
            'dev': 'realiDB',
            'qa': 'realiDB',
            'staging': 'realidb-staging',
            'prod': 'realiDB'},
        'cluster': {
            'dev': 'reali-dev-shard-00-00-kziwr.mongodb.net:27017, reali-dev-shard-00-01-kziwr.mongodb.net:27017, reali-dev-shard-00-02-kziwr.mongodb.net:27017',
            'qa': 'qa-shard-00-00-kziwr.mongodb.net:27017,qa-shard-00-01-kziwr.mongodb.net:27017,qa-shard-00-02-kziwr.mongodb.net:27017',
            'staging': 'realidb-staging-shard-00-00-kziwr.mongodb.net:27017,realidb-staging-shard-00-01-kziwr.mongodb.net:27017, realidb-staging-shard-00-02-kziwr.mongodb.net:27017',
            'prod': 'prod-ghost-shard-00-00-kziwr.mongodb.net:27017,prod-ghost-shard-00-01-kziwr.mongodb.net:27017,prod-ghost-shard-00-02-kziwr.mongodb.net:27017'},
        'auth': {
            'qa': 'ssl=true&ssl_cert_reqs={}&replicaSet=qa-shard-0&authSource=admin'.format('CERT_NONE'),
            'dev': 'ssl=true&ssl_cert_reqs={}&replicaSet=reali-dev-shard-0&authSource=admin'.format('CERT_NONE'),
            'staging': 'ssl=true&ssl_cert_reqs={}&replicaSet=realidb-staging-shard-0&authSource=admin'.format(
                'CERT_NONE'),
            'prod': 'ssl=true&ssl_cert_reqs={}&replicaSet=prod-ghost-shard-0&authSource=admin&authMechanism=SCRAM-SHA-1'.format(
                'CERT_NONE'),
            'prod_': 'ssl=true&ssl_cert_reqs={}&replicaSet=reali-dev-shard-0&authSource=admin'.format('CERT_NONE'),
        }
    },
    'Properties': {
        'dbname': {
            'dev': 'editorialTransformerDB',
            'qa': 'editorialTransformerDB',
            'staging': 'editorialTransformerDB',
            'prod': 'editorialTransformerDB'},
        'cluster': {
            'dev': 'dev-data-shard-00-00-kziwr.mongodb.net:27017, dev-data-shard-00-01-kziwr.mongodb.net:27017, dev-data-shard-00-02-kziwr.mongodb.net:27017',
            'qa': 'qa-shard-00-00-kziwr.mongodb.net:27017,qa-shard-00-01-kziwr.mongodb.net:27017,qa-shard-00-02-kziwr.mongodb.net:27017',
            'staging': 'realidb-staging-shard-00-00-kziwr.mongodb.net:27017, realidb-staging-shard-00-01-kziwr.mongodb.net:27017, realidb-staging-shard-00-02-kziwr.mongodb.net:27017',
            'prod': 'prod-editorial-shard-00-00-kziwr.mongodb.net:27017,prod-editorial-shard-00-01-kziwr.mongodb.net:27017,prod-editorial-shard-00-02-kziwr.mongodb.net:27017'},
        'auth': {
            'dev': 'ssl=true&ssl_cert_reqs={}&replicaSet=dev-data-shard-0&authSource=admin'.format('CERT_NONE'),
            'qa': 'ssl=true&ssl_cert_reqs={}&replicaSet=qa-shard-0&authSource=admin'.format('CERT_NONE'),
            'staging': 'ssl=true&ssl_cert_reqs={}&replicaSet=realidb-staging-shard-0&authSource=admin'.format(
                'CERT_NONE'),
            'prod': 'ssl=true&ssl_cert_reqs={}&replicaSet=prod-editorial-shard-0&authSource=admin&authMechanism=SCRAM-SHA-1'.format(
                'CERT_NONE')
        }
    }
}
