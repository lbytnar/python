#!/usr/bin/env python
"""
AWS-Rename-instances.py for AWS Lambda.

This script checks if every production instance has its availibility zone
in name. E.g. "prod-web" -> "prod-web 1c".
"""

__author__ = "Lukasz Bytnar"
__copyright__ = "Copyright 2018"
__version__ = "0.0.1"
__maintainer__ = "Lukasz Bytnar"
__email__ = ""
__status__ = "Production"

import boto3

INSTANCE_FILTERS = [
        {"Name": "tag:Name", "Values": ["prod-*"]},
        {"Name": "instance-state-name", "Values": ['running']}
    ]


def ec2_connect():
    """Return connection object for ec2."""
    return boto3.client('ec2')


def get_tag(instance, tag_name):
    """Retrieve instance 'tag_name' tag from the instance."""
    # gets the first value from the iterator, iow: first tag only.
    return next(
        (
            t['Value'] for t in instance['Tags'] if t['Key'] == tag_name
        ),
        None
    )


def tag_resources(ec2, resource_id, label):
    """Add tag to resource."""
    ec2.create_tags(
        Resources=(resource_id,),
        Tags=[
            {'Key': 'Name', 'Value': label},
        ])
    print "Creating tag for resource_id %s with Name:%s label!" % (resource_id,
                                                                   label)


def get_instances(ec2):
    """Return instances after applying filter."""
    reservations = ec2.describe_instances(
      Filters=INSTANCE_FILTERS).get('Reservations', [])
    instances = sum(
        [
            [i for i in r['Instances']]
            for r in reservations
        ], [])
    return instances


def lambda_handler(event, context):
    """Handler for a lambda funcion."""
    ec2 = ec2_connect()
    instances = get_instances(ec2)

    changes = 0

    for i in instances:
        instanceName = get_tag(i, 'Name')
        if instanceName in ['prod-tftp']:
            continue

        instanceEnd = instanceName.split(' ')[-1]
        if instanceEnd not in ['1a', '1b', '1c', '1d', '1e', '1f']:
            print 'Adding AZ to ' + instanceName
            az = ' ' + i['Placement']['AvailabilityZone'].split('-')[-1]
            instanceId = i['InstanceId']
            tag_resources(ec2, instanceId, instanceName + az)
            changes += 1

    if not changes:
        print 'All prod-* names correct. No changes made!'


lambda_handler(None, None)
