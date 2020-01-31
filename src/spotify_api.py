#! /usr/bin/env python3

import requests
from datetime import datetime
import configparser


def refresh_access_token(client_id, client_secret, refresh_token):
    auth_token_url = 'https://accounts.spotify.com/api/token'

    payload = {'client_id': client_id,
               'client_secret': client_secret,
               'refresh_token': refresh_token,
               'grant_type': 'refresh_token',
               }

    r = requests.post(auth_token_url, data=payload)

    if r.status_code == 200:
        tokens = r.json()
        access_token = tokens['access_token']
        refresh_token = tokens.get('refresh_token', refresh_token)

        ts = datetime.strptime(r.headers['date'],
                               '%a, %d %b %Y %X %Z').strftime('%s')

        config = configparser.ConfigParser()
        config.read('../secrets.cfg')

        # here there might be concurrency issues if there are ever multiple
        # workers involved...

        config['spotify_secrets']
        config['spotify_secrets']['SPOTIFY_ACCESS_TOKEN'] = access_token
        config['spotify_secrets']['SPOTIFY_REFRESH_TOKEN'] = refresh_token
        config['spotify_secrets']['SPOTIFY_REFRESH_TIME'] = ts

        with open('../secrets.cfg', 'w') as f:
            config.write(f)
    else:
        raise requests.HTTPError('Error: response: {}'.format(r.text))

    return access_token


def check_for_refresh():
    config = configparser.ConfigParser()
    config.read('../secrets.cfg')

    secrets = config['spotify_secrets']

    refresh_time = int(secrets['SPOTIFY_REFRESH_TIME'])
    current_time = int(datetime.now().strftime('%s'))

    if current_time - refresh_time > 3540:
        access_token = refresh_access_token(secrets['client_id'],
                                            secrets['client_secret'])

    return access_token
