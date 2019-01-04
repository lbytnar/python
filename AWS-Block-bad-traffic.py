#!/usr/bin/env python
"""
AWS-Block-bad-traffic.py for AWS Lambda.

This script gets IPs from graylog that had over 2000 hits and ban them for
2 weeks. After that it removes them from ban list.
"""

__author__ = "Lukasz Bytnar"
__copyright__ = "Copyright 2018"
__version__ = "0.0.1"
__maintainer__ = "Lukasz Bytnar"
__email__ = ""
__status__ = "Production"

import boto3
from urllib2 import Request, urlopen, URLError
from base64 import b64encode
import json
from datetime import datetime, date, timedelta
import os

ENDPOINT = os.environ['ENDPOINT']
API_URL = os.environ['API_URL']
TOKEN = os.environ['TOKEN']
TOKEN_B64 = b64encode(TOKEN)
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
HIT_COUNT = int(os.environ['HIT_COUNT'])
LOG_FILE_DIR = '/tmp/'
LOG_FILE = os.environ['LOG_FILE']
LOG_FILE_DATA = []
REMOVE_AFTER = int(os.environ['REMOVE_AFTER'])
REMOVE_AFTER_DELTA = timedelta(days=REMOVE_AFTER)
NACL_ID = os.environ['NACL_ID']
NACL_FILTER = [
    {'Name': 'association.network-acl-id',
     'Values': [NACL_ID]}
]


def ec2_connect():
    """Return connection object for ec2."""
    return boto3.client('ec2')


def s3_connect():
    """Return connection object for s3."""
    return boto3.client('s3')


def get_free_rules_number(acl, deleted):
    """Find the next acl rule number."""
    rules_list = acl['NetworkAcls'][0]['Entries']
    rules_used = [rule['RuleNumber']
                  for rule in rules_list if not rule['Egress']]
    rules_free = [num for num in range(1, 100) if num not in rules_used]
    rules_free = rules_free + deleted
    rules_free.sort()
    return rules_free


def get_blocked_ips(acl, ips_removed):
    """Get the list of IPs that are denied already."""
    blocked_ips = []
    for rule in acl['NetworkAcls'][0]['Entries']:
        if rule['RuleAction'] == 'deny' and not rule['Egress']:
            blocked_ips.append(rule['CidrBlock'].split('/')[0])
    return list(set(blocked_ips) - set(ips_removed))


def read_log_data():
    """Read data from log file."""
    global LOG_FILE_DIR
    global LOG_FILE_DATA
    global LOG_FILE

    with open(LOG_FILE_DIR + LOG_FILE, 'r') as logfile:
        LOG_FILE_DATA = logfile.readlines()


def get_ips_to_remove():
    """Retrieve IPs that should be unblocked."""
    global LOG_FILE_DATA
    read_log_data()
    ips_to_remove = []

    # iterate over copy of original LOG_FILE_DATA
    for line in list(LOG_FILE_DATA):
        rule_date = datetime.strptime(line.split()[1], '%d/%m/%y').date()
        if date.today() - rule_date > REMOVE_AFTER_DELTA:
            ips_to_remove.append(line.split()[0])
            LOG_FILE_DATA.remove(line)
    return ips_to_remove


def delete_rules(ec2, ips, acl):
    """Remove NACL rule."""
    global LOG_FILE_DATA
    rule_deleted = []
    for rule in acl['NetworkAcls'][0]['Entries']:
        if rule['Egress'] or rule['RuleAction'] == 'allow':
            continue

        rule_num = rule['RuleNumber']
        rule_ip = rule['CidrBlock'].split('/')[0]
        if rule_ip in ips:
            print 'Removed ' + rule_ip + '/32 from NACL.'
            ec2.delete_network_acl_entry(
               Egress=False,
               NetworkAclId=NACL_ID,
               RuleNumber=rule_num
            )
            rule_deleted.append(rule_num)

    return rule_deleted


def lambda_handler(event, context):
    """Handler for a lambda funcion."""
    global LOG_FILE_DATA
    ec2 = ec2_connect()
    s3 = s3_connect()
    today = datetime.now().strftime("%d/%m/%y %H:%M")

    request = Request(ENDPOINT + API_URL)
    request.add_header('Authorization', 'Basic %s' % TOKEN_B64)
    data = {}
    s3.download_file(S3_BUCKET_NAME, LOG_FILE, LOG_FILE_DIR + LOG_FILE)
    nacl = ec2.describe_network_acls(Filters=NACL_FILTER)
    ips_to_remove = get_ips_to_remove()
    deleted = delete_rules(ec2, ips_to_remove, nacl)
    free_rules = get_free_rules_number(nacl, deleted)
    blocked_ips = get_blocked_ips(nacl, ips_to_remove)

    try:
        response = urlopen(request)
        data = json.load(response)
    except URLError, e:
        print 'error: ', e

    if data:
        for ip, count in data['result']['terms'].items():
            if count > HIT_COUNT:
                if ip in blocked_ips:
                    print ip + ' already blocked!'
                else:
                    response = ec2.create_network_acl_entry(
                        CidrBlock=ip+'/32',
                        Egress=False,
                        Protocol='-1',
                        NetworkAclId=NACL_ID,
                        PortRange={'From': 0, 'To': 65535},
                        RuleAction='deny',
                        RuleNumber=free_rules.pop()
                    )
                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print 'Added %-15s %s' % (ip, today)
                        LOG_FILE_DATA.append('%-15s %s\n' % (ip, today))

    with open(LOG_FILE_DIR + LOG_FILE, 'w') as bad_ips_file:
        for line in LOG_FILE_DATA:
            bad_ips_file.write("%s" % line)
    s3.upload_file(LOG_FILE_DIR + LOG_FILE, S3_BUCKET_NAME, LOG_FILE)


# lambda_handler(None, None)
