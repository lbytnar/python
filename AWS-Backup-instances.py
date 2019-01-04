#!/usr/bin/env python
"""
AWS-Backup-instances.py for AWS Lambda.

It creates image backup for instances. If corresponding Auto Scaling Group
exists, it creates Launch Configuration with newly created AMI, replace LC at
Autoscaling Group and remove old LC.

It also cleans AMI and snapshots older than RETENTION.

Finally, if backups is successful, it sends SNS notification.
"""

__author__ = "Lukasz Bytnar"
__copyright__ = "Copyright 2018"
__version__ = "0.0.2"
__maintainer__ = "Lukasz Bytnar"
__email__ = ""
__status__ = "Production"

import boto3
from botocore.exceptions import ClientError
import datetime
import re
import base64
import time

INSTANCE_FILTERS = [
   {'Name': 'tag:Backup', 'Values': ['Yes']}
]

OWNER_ID = ''
REGION = 'eu-west-1'
SNS_TOPIC = ''
SNAPSHOT_DESC_PATTERN = re.compile(
    'Created by CreateImage\(i-[a-z0-9]{8,}\) for '
    '(ami-[a-z0-9]{8,}) from vol-[a-z0-9]{8,}')

RETENTION = datetime.timedelta(days=5)

DESC_NAMES = {
    'build-jenkins': 'Jenkins',
    'build-artifactory': 'Artifactory'
}

DATE_FORMAT = '%d-%m-%Y'


def ec2_connect():
    """Return connection object for ec2."""
    return boto3.client('ec2')


def asc_connect():
    """Return connection object for autoscaling."""
    return boto3.client('autoscaling')


def sns_connect():
    """Return connection objcet for Simple Notification Service."""
    return boto3.client('sns')


def get_tag(instance, tag_name):
    """Retrieve instance 'tag_name' tag from the instance."""
    # gets the first value from the iterator, iow: first tag only.
    return next(
        (
            t['Value'] for t in instance['Tags'] if t['Key'] == tag_name
        ),
        None
    )


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


def tag_resources(ec2, resource_id, label):
    """Add tag to resource."""
    ec2.create_tags(
        Resources=(resource_id,),
        Tags=[
            {'Key': 'Name', 'Value': label},
        ])
    print "Creating tag for resource_id %s with Name:%s label!" % (resource_id,
                                                                   label)


def get_snapshots(ec2):
    """Return list of ami_id with the corresponding snapshots."""
    response = ec2.describe_snapshots(OwnerIds=[OWNER_ID], MaxResults=1000,
                                      Filters=[
                                      {'Name': 'status',
                                       'Values': ['completed']}
                                      ])['Snapshots']

    all_snapshots = dict()
    for s in response:
        snapshot_id = s['SnapshotId']
        snapshot_desc = s['Description']
        m = SNAPSHOT_DESC_PATTERN.match(snapshot_desc)
        if m is None:
            pass
        else:
            ami_id = m.group(1)
            if ami_id not in all_snapshots:
                all_snapshots[ami_id] = []
            all_snapshots[ami_id].append(snapshot_id)
    return all_snapshots


def remove_backup(ec2, image_id, snapshots):
    """Remove 'image_id' AMI and all snapshots (that belongs to AMI)."""
    print("- Remove AMI %s " % image_id)
    ec2.deregister_image(ImageId=image_id)
    for s in snapshots:
        print("- Remove snapshot %s" % s)
        ec2.delete_snapshot(SnapshotId=s)


def backup_instance(ec2, instance, name):
    """Create AMI of the instance without a reboot."""
    state = instance['State']['Name']

    today_epoch_time = datetime.date.today().strftime('%s')

    print("+ Create backup of %s (%s), state: %s" %
          (name, instance['InstanceId'], state))

    desc_name = DESC_NAMES.get(name, 'Unnamed snapshot')

    ami_desc = "%s %s" % (desc_name + ' Daily',
                          datetime.date.today().strftime(DATE_FORMAT))
    ami_name = name + '-' + today_epoch_time

    noreboot = state == 'running'
    print("+ Call create_image, Name=%s, Desc=%s, NoReboot=%s" %
          (ami_name, ami_desc, noreboot))
    try:
        image = ec2.create_image(InstanceId=instance['InstanceId'],
                                 Name=ami_name, Description=ami_desc,
                                 NoReboot=noreboot)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidAMIName.Duplicate':
            print("  => " + e.response['Error']['Message'])
            return False

    img_response_code = image['ResponseMetadata']['HTTPStatusCode']
    print("+ EC2 response: HTTPStatusCode %s" % img_response_code)
    if img_response_code != 200:
        return -1

    if(image['ImageId']):
        print("Waiting for image to become available: " + image['ImageId'])
        # waiter = ec2.get_waiter('image_available')
        # waiter.wait(ImageIds=[image['ImageId']])
        snaps = []
        while snaps == []:
            time.sleep(5)
            snaps = ec2.describe_images(Owners=['self'], Filters=[
                                        {'Name': 'image-id',
                                         'Values': [image['ImageId']]}
                                        ])['Images'][0]['BlockDeviceMappings']
        for s in snaps:
            tag_resources(ec2, s['Ebs']['SnapshotId'], ami_desc)
        tag_resources(ec2, image['ImageId'], ami_desc)
    return image['ImageId']


def get_images(ec2):
    """
    The function collects a map of all existing private images by name.

    Return the list of images.
    """
    response = ec2.describe_images(Owners=['self'], Filters=[
        {'Name': 'image-type', 'Values': ['machine']},
        {'Name': 'root-device-type', 'Values': ['ebs']},
        {'Name': 'state', 'Values': ['available']}
    ])['Images']

    all_images = dict()
    for i in response:
        if not i['Public']:  # skip public images
            name = i['Name']
            image_id = i['ImageId']
            created_at = datetime.datetime.strptime(
                i['CreationDate'].split('T')[0], "%Y-%m-%d"
            ).date()

            pos = name.rfind('-')
            if pos > 0 and name[pos + 1:].isdigit():  # basic checks
                name = name[0:pos]
                if name not in all_images:
                    all_images[name] = []
                all_images[name].append(
                    {'id': image_id, 'created_at': created_at}
                )

    return all_images


def update_autoscaling_group(asc, asg_name, lc_name):
    """Update Auto Scaling Group with the Launch Configuration."""
    response = asc.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        LaunchConfigurationName=lc_name
    )

    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print 'AutoScaling Status: ', response_code

    if response_code == 200:
        return 0
    else:
        return -1


def delete_launch_configuration(asc, lc_name):
    """Delete launch configuration with lc_name."""
    asc.delete_launch_configuration(
        LaunchConfigurationName=lc_name
    )


def update_launch_configuration(asc, lc_name, ami_id):
    """Update launch configuration image from newly created AMI."""
    lc_config = asc.describe_launch_configurations(
        LaunchConfigurationNames=[lc_name]
    )['LaunchConfigurations']

    if len(lc_config) > 0:
        lc_config = lc_config[0]
    else:
        return -1

    PARAM_LIST = {}
    for param in lc_config:
        # ignore this parameters for LC config
        if param in ['LaunchConfigurationARN', 'CreatedTime', 'RamdiskId',
                     'KernelId']:
            continue

        PARAM_LIST[param] = lc_config[param]

    today = ' ' + datetime.date.today().strftime(DATE_FORMAT)
    lc_name_today = lc_config['LaunchConfigurationName'].split()[0] + today
    update = {'LaunchConfigurationName': lc_name_today, 'ImageId': ami_id,
              'UserData': base64.b64decode(lc_config['UserData'])}

    PARAM_LIST.update(update)

    response = asc.create_launch_configuration(**PARAM_LIST)
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print 'LaunchConfiguration Status: ', response_code

    if response_code == 200:
        return lc_name_today
    else:
        return -1


def lambda_handler(event, context):
    """Handler for a lambda funcion."""
    ec2 = ec2_connect()
    asc = asc_connect()
    sns = sns_connect()

    all_images = get_images(ec2)
    all_snapshots = get_snapshots(ec2)
    instances = get_instances(ec2)

    today = datetime.date.today()
    day_ago = (today - datetime.timedelta(days=1)).strftime(DATE_FORMAT)

    for i in instances:
        name = get_tag(i, 'Name')

        print('Instance %s, status: %s' % (name, i['State']['Name']))
        if name in all_images:
            images = all_images[name]
            images.sort(key=lambda r: r['created_at'])
            dates = [r['created_at'] for r in images]
            print("%d images since %s till %s" % (
                len(dates), dates[0].isoformat(), dates[-1].isoformat()))
        else:
            images = []
            print("no backup images are found")

        # Create AMI of instances
        if len(images) > 0 and images[len(images)-1]['created_at'] == today:
            print("  => Skip %s (%s) - a fresh backup already exists" %
                  (name, i['InstanceId']))
        else:
            ami_id = backup_instance(ec2, i, name)
            lc_name_updated = update_launch_configuration(
                    asc, name + '-lc ' + day_ago, ami_id)
            if lc_name_updated != -1:
                res = update_autoscaling_group(asc, name + '-asg',
                                               lc_name_updated)
                if not res:
                    delete_launch_configuration(asc, name + '-lc ' + day_ago)
                    # Send notification that backup was completed.
                    sns.publish(
                        TopicArn=SNS_TOPIC,
                        Subject=name + ' Daily Backup',
                        Message=name + ' backup completed successfully.'
                    )
                else:
                    return 1
            else:
                return 1

        # Remove old Backups
        for img in images:
            if today - img['created_at'] > RETENTION:
                remove_backup(ec2, img['id'], all_snapshots[img['id']])

# Uncomment to test
# lambda_handler(None, None)
