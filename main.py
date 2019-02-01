import fusepass
import logging
import os
import subprocess
import getpass

from evernote.api.client import EvernoteClient

import config

EVERNOTE_TOKEN_FILE = '.evernote_token'
OAUTH_URL = 'http://localhost'


def main():
    logging.basicConfig(level=logging.DEBUG)
    if not mount_point_exists():
        mount_point_create()

    client = EvernoteClient(token=get_evernote_token())
    # note_store = client.get_note_store()
    # notebooks = note_store.listNotebooks()
    # print(notebooks[0])
    # note_filter = NoteFilter()
    # note_filter.notebookGuid = notebooks[0].guid
    # note_list = note_store.findNotes(note_filter, 0, 1001)
    # print(note_list)

    fusepass.EvernoteFuse(config.MOUNT_POINT, client)


def mount_point_exists():
    return os.path.isdir(config.MOUNT_POINT)


def mount_point_create():
    print('Creating mount point requires root')
    subprocess.call(['sudo', 'mkdir', config.MOUNT_POINT])
    subprocess.call(['sudo', 'chown', getpass.getuser(), config.MOUNT_POINT])
    print('Mount point created:', config.MOUNT_POINT)


def parse_query_string(authorize_url):
    args = authorize_url.split('?')
    values = {}
    if len(args) == 1:
        raise Exception('Invalid Authorization URL')
    for pair in args[1].split('&'):
        key, value = pair.split('=', 1)
        values[key] = value
    return values


def get_evernote_token():
    if os.path.exists(EVERNOTE_TOKEN_FILE):
        token_file = open(EVERNOTE_TOKEN_FILE, 'r')
        token = token_file.read()
        token_file.close()
        return token
    else:
        token = request_evernote_token()
        token_file = open(EVERNOTE_TOKEN_FILE, 'w')
        token_file.write(token)
        token_file.close()
        return token


def request_evernote_token():
    client = EvernoteClient(
        consumer_key=config.CONSUMER_KEY,
        consumer_secret=config.CONSUMER_SECRET,
        sandbox=config.SANDBOX
    )
    request_token = client.get_request_token(OAUTH_URL)
    print('Paste this URL in your browser and login')
    print(client.get_authorize_url(request_token))
    print('Paste the URL after login here:')
    auth_url = input()
    values = parse_query_string(auth_url)
    auth_token = client.get_access_token(
        request_token['oauth_token'],
        request_token['oauth_token_secret'],
        values['oauth_verifier']
    )
    print('Auth done')
    return auth_token


if __name__ == '__main__':
    main()
