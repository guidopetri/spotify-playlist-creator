#! /usr/bin/env python3

import requests
import os
from datetime import datetime


def refresh_access_token(client_id, client_secret):
    auth_token_url = 'https://accounts.spotify.com/api/token'

    refresh_token = os.environ['SPOTIFY_REFRESH_TOKEN']

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

        # update access/refresh token
        if int(ts) > int(os.environ.get('SPOTIFY_REFRESH_TIME', 0)):
            os.environ['SPOTIFY_ACCESS_TOKEN'] = access_token
            os.environ['SPOTIFY_REFRESH_TOKEN'] = refresh_token
            os.environ['SPOTIFY_REFRESH_TIME'] = ts
    else:
        raise requests.HTTPError('Error: response: {}'.format(r.text))

    return


def check_for_refresh():
    refresh_time = int(os.environ.get('SPOTIFY_REFRESH_TIME', 0))
    current_time = int(datetime.now().strftime('%s'))

    if current_time - refresh_time > 3540:
        import json

        with open('secrets.cfg', 'r') as f:
            secrets = json.load(f)

        refresh_access_token(secrets['client_id'], secrets['client_secret'])

    return os.environ['SPOTIFY_ACCESS_TOKEN']
