aws_new_account = True

vendors2_test = ['mls_listings', 'bayeast', 'crmls', 'sdmls', 'sfar', 'metrolist', 'bareis']

extraction_flows = {
    'UPDATED':
        {
            'priority': 'HIGH',
            'retrieve_by': 'DATE',
            'latest_by': 'mls_update_date'
        },
    'MISSED':
        {
            'priority': 'HIGH',
            'retrieve_by': 'DATE',
            'latest_by': 'last_update_date'
        },
    'ON_MARKET':
        {
            'priority': 'HIGH',
            'retrieve_by': 'STATUS',
            'latest_by': None
        },
    'OFF_MARKET':
        {
            'priority': 'LOW',
            'retrieve_by': 'STATUS',
            'latest_by': None
        }
}

DB_MONGO = {
    'user': 'RealiDB',
    'password': 'g9CBgF0GYwdSTwa6',
    'listings': {
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
