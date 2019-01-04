#!/usr/bin/env python
import boto3
from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from pymongo import errors

def ec2connect():
    """ Returns connection object for ec2. """

    return boto3.client('ec2')

def list_instances(ec2):
    """ Returns all instances objects that have tag Backup/backup with arbitrary value as a list of dictionaries. """

    reservations = ec2.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['backup', 'Backup']},
        ]
    )['Reservations']

    instances = sum(
    [
        [i for i in r['Instances']]
        for r in reservations
    ], [])

    return instances

def backup_volume(ec2,instances):
    """
        Makes snapshot of mongo volume on primary node.
        Function presumes that mongo device is /dev/sdf

        It takes ec2 connection object and list of dictionary instances.
        When successfully completed returns True.
    """

    for instance in instances:
        retention = get_retention(instance)
        if not is_master(instance['PrivateIpAddress']):
            #make snapshot only on primary
            continue

        for dev in instance['BlockDeviceMappings']:
            if dev.get('Ebs', None) is None:
                # skip non-EBS volumes
                continue

            retention = get_retention(instance)
            now = datetime.today()
            delete_date_days = (now + timedelta(days=retention['days'])).strftime('%Y-%m-%d')
            delete_date_weeks = (now + timedelta(weeks=retention['weeks'])).strftime('%Y-%m-%d')
            delete_date_months = (now + relativedelta(months=retention['months'])).strftime('%Y-%m-%d')
            desc_date = now.strftime('%Y-%m-%d.%H:%M:%S')


            # all mongo disks are sdf
            if dev['DeviceName'] == '/dev/sdf':
                    vol_id = dev['Ebs']['VolumeId']

                    # Make sure that only one snapshot is taken, whether daily, weekly or monthly.
                    if now.strftime('%d') == '01':
                        print "Creating snapshot of %s volume that will be retain for %d months" % (vol_id, retention['months'])
                        snap = make_snapshot(ec2,vol_id, retention['months'], "MongoMonthlyBackupSnapshot-"+desc_date)
                        tag_snapshot(ec2, snap['SnapshotId'], delete_date_months)
                    elif now.strftime('%a') == 'Sun':
                        print "Creating snapshot of %s volume that will be retain for %d weeks" % (vol_id, retention['weeks'])
                        snap = make_snapshot(ec2,vol_id, retention['weeks'], "MongoWeeklyBackupSnapshot-"+desc_date)
                        tag_snapshot(ec2, snap['SnapshotId'], delete_date_weeks)
                    else:
                        print "Creating snapshot of %s volume that will be retain for %d days" % (vol_id, retention['days'])
                        snap = make_snapshot(ec2,vol_id, retention['days'], "MongoDailyBackupSnapshot-"+desc_date)
                        tag_snapshot(ec2, snap['SnapshotId'], delete_date_days)

    return True

def is_master(nodeIP):
    """
        Takes clientIP and returns
        - True if node is primary,
        - False if is secondary or arbiter,
        - None if connection fails
    """

    # no need to be authenticated for asking if 'isMaster'
    uri = 'mongodb://'+nodeIP+':27017'
    try:
       client = MongoClient(uri)
       result = client.admin.command('isMaster')
    except errors.ConnectionFailure:
        return None

    return result['ismaster']


def get_retention(instance):
    """ Takes dictionary instance and reads Retention tag for days, weeks and months.
        If any tag is not set up it taks defaut as 7, 4, 12 for days, weeks and months recpectively.
    """

    retention = {}

    try:
        retention_days = [
            int(t.get('Value')) for t in instance['Tags']
            if t['Key'] == 'Retention_daily'][0]
    except IndexError:
        retention_days = 7
    retention['days'] = retention_days

    try:
        retention_weeks = [
            int(t.get('Value')) for t in instance['Tags']
            if t['Key'] == 'Retention_weekly'][0]
    except IndexError:
        retention_weeks = 4
    retention['weeks'] = retention_weeks

    try:
        retention_months = [
            int(t.get('Value')) for t in instance['Tags']
            if t['Key'] == 'Retention_monthly'][0]
    except IndexError:
        retention_months = 12
    retention['months'] = retention_months


    return retention

def make_snapshot(ec2,vol,retention,description):
    """
        Makes snapshot of 'vol' with description.
        Returns dictionary with the result.
    """

    snap = ec2.create_snapshot(VolumeId=vol,Description=description)
    return snap


def tag_snapshot(ec2, snap_id, label):
    """
        Takes snapshot id and date YYYY-MM-DD when snapshot should be deleted. Then adds tag DeleteOn
    """

    ec2.create_tags(
    Resources=(snap_id,),
    Tags=[
        {'Key': 'DeleteOn', 'Value': label},
    ])
    print "Creating tag for snap %s with %s label!" % ( snap_id, label )

def remove_old_snapshots(ec2):
    """
        Build a list with snapshot's tagged with 'DeleteOn' equals to today's date.
        Then removes all snapshots that were destined to be deleted today.
    """

    delete_on =  date.today().strftime('%Y-%m-%d')
    filters = [
        {'Name': 'tag-key', 'Values': ['DeleteOn']},
        {'Name': 'tag-value', 'Values': [delete_on]},
    ]
    snapshot_response = ec2.describe_snapshots(Filters=filters)

    for snap in snapshot_response['Snapshots']:
        print "Deleting snapshot %s" % snap['SnapshotId']
        ec2.delete_snapshot(SnapshotId=snap['SnapshotId'])

    return True


def mongo_backup(event, context):
      """
        Connects to ec2, build list of all potenial instances,
        backup all mongo primary volumes and groom old backupsself.
      """


      ec2 = ec2connect()

      #get list of instances with `Backup` tag
      inst = list_instances(ec2)
      backup_volume(ec2,inst)
      remove_old_snapshots(ec2)
      print "Backup finished successfully!"
      return True

if __name__ == '__main__':
    handlers.invoking(mongo_backup)
