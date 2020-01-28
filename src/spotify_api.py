#! /usr/bin/env python3

import requests
import os
from datetime import datetime


def refresh_access_token(client_id, client_secret, refresh_token):
    auth_url_token = 'https://accounts.spotify.com/api/token'

    payload = {'client_id': client_id,
               'client_secret': client_secret,
               'refresh_token': refresh_token,
               'grant_type': 'refresh_token',
               }

    r = requests.post(auth_url_token, data=payload)

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
