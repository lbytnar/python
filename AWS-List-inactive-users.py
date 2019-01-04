#!/usr/bin/env python

__author__ = "Lukasz Bytnar"
__copyright__ = "Copyright 2018"
__version__ = "0.0.1"
__maintainer__ = "Lukasz Bytnar"
__email__ = ""
__status__ = "Development"


import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from datetime import timedelta, datetime
import argparse



iam = boto3.resource('iam')
iam2 = boto3.client('iam')

all_users = iam.users.all()

# print dir(iam.users)
DATE_FORMAT = '%d-%m-%Y %H:%M:%S+00:00'
now = datetime.now()
check_period = timedelta(days=90)
policy_time = now - check_period

def user_lastused_pass(users, verbose=False):
    noconsole_users = get_noconsole_users(users)
    lastused = []
    for user in users:
        if user.user_name in noconsole_users:
            continue
        if user.password_last_used == None:
            if not verbose:
                lastused.append(user.user_name)
            else:
                lastused.append((user.user_name, -1))
            continue
        if policy_time > user.password_last_used.replace(tzinfo=None):
            if not verbose:
                lastused.append(user.user_name)
            else:
                lastused.append((user.user_name, (now - user.password_last_used.replace(tzinfo=None)).days))
    return lastused

def check_user_keys(users, verbose=False):
    lastkeys = []
    for user in users:
        user_keys = iam2.list_access_keys(UserName=user.user_name)['AccessKeyMetadata']
        if len(user_keys) > 0:
            print user_keys
            for key in user_keys:
                if key['Status'] == 'Active':
                    key_data = iam2.get_access_key_last_used(AccessKeyId=key['AccessKeyId'])
                    print key_data
                    if policy_time > key_data['AccessKeyLastUsed']['LastUsedDate'].replace(tzinfo=None):
                        if not verbose:
                            lastkeys.append(user.user_name)
                        else:
                            lastkeys.append((user.user_name, (policy_time - key_data['AccessKeyLastUsed']['LastUsedDate'].replace(tzinfo=None)).days))
    return lastkeys

def is_user_active_key(username):
        user_keys = iam2.list_access_keys(UserName=username)['AccessKeyMetadata']
        if len(user_keys) > 0:
            for key in user_keys:
                if key['Status'] == 'Active':
                    return True
        else:
            return False


def get_noconsole_users(users):
    noconsole_users = []
    for user in users:
        profile = user.LoginProfile()
        try:
            profile.load()
        except ClientError:
            noconsole_users.append(user.user_name)
    return noconsole_users

def get_nokeys_users(users):
    nokeys_users = []
    for user in users:
        if not is_user_active_key(user.user_name):
            nokeys_users.append(user.user_name)
    return nokeys_users

def get_inactive_user(users):
    console = get_noconsole_users(users)
    keys = get_nokeys_users(users)
    return [user for user in console if user in keys]

def get_user_activity(users):
    # console = user_lastused_pass(users)
    keys = check_user_keys(users)
    # print console
    print keys
    return []
    # return [user for user in console if user in keys]

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--no-console', help='show users with no console access', action='store_true')
parser.add_argument('-t', '--no-recent-activity', help='show users with last activity longer than 90 days', action='store_true')
parser.add_argument('-d', '--disabled', help='show users with no console login and no access key', action='store_true')
parser.add_argument('-l', '--last-active', help='show users that didnt use console or access key in last 90 days', action='store_true')
parser.add_argument('-v', '--verbose', help='show extra information', action='store_true')
args = parser.parse_args()
if args.no_console:
    print ("\n").join(get_noconsole_users(all_users))
elif args.no_recent_activity:
    if not args.verbose:
        print ("\n").join(user_lastused_pass(all_users))
    else:
        print ("\n").join([user[0] + ' (' + str(user[1]) + ' days)' for user in user_lastused_pass(all_users, args.verbose)])
        #print user_lastused_pass(all_users, args.verbose)
elif args.disabled:
    print ("\n").join(get_inactive_user(all_users))
elif args.last_active:
    print ("\n").join(get_user_activity(all_users))

# for user in iam.users.all():
#     # print user
#     check_user_keys(user.user_name)
#     profile = user.LoginProfile()
#     try:
#         profile.load()
#     except ClientError as e:
#         # print(e.__dict__)
#         #print user.user_name, '(NO CONSOLE ACCESS)'
#         continue
#     # print profile
#     if user.password_last_used is not None:
#         if user_lastused_pass(user):
#             # print dir(policy_time - user.password_last_used.replace(tzinfo=None))
#             print user.user_name, (policy_time - user.password_last_used.replace(tzinfo=None)).days
#             # print 'Last used: ', user.password_last_used
#             # print 'Create_date: ', user.create_date
