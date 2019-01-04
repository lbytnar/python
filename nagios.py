#!/usr/bin/env python
"""
Nagios.py config generator.

Generates host template and service template file from the given dictionary.
The scrits queries AWS API to retrieve all production instances and transform
them to nagios config. It also verifies config, and check if it was changed. In
this case it reloads Nagios to apply new configuration.
"""

__author__ = "Lukasz Bytnar"
__copyright__ = "Copyright 2018"
__version__ = "0.0.1"
__maintainer__ = "Lukasz Bytnar"
__email__ = ""
__status__ = "Production"


import boto3
import shutil
import subprocess
import filecmp

nagios_cfg_path = '/usr/local/nagios/etc/'
nagios_hosts_path = nagios_cfg_path + 'hosts.cfg'
nagios_services_path = nagios_cfg_path + 'services.cfg'

# host template
host_template = """define host {
  use                     linux-server
  host_name               %s
  check_command           check-host-alive
  address                 %s
}
"""

# service template
service_template = """define service {
  use                   generic-service
  host_name             %s
  service_description   %s
  check_command         check_nrpe!%s
}
"""

host_cfg = ''
service_cfg = ''

host_list = {}
service_list = {
  'cpu': {'desc': 'CPU Load', 'cmd': 'check_load'},
  'log_check': {'desc': 'Critical Log', 'cmd': 'check_larevel'},
  'users': {'desc': 'Current Users', 'cmd': 'check_users'},
  'disk': {'desc': 'Disk', 'cmd': 'check_hda1'},
  'php': {'desc': 'PHP Processes', 'cmd': 'check_php'},
  'http_8081': {'desc': 'HTTP:8081', 'cmd': 'check_health8081'},
  'http_81': {'desc': 'HTTP:81', 'cmd': 'check_health81'},
  'ssh': {'desc': 'SSH', 'cmd': 'check_ssh'},
  'proc': {'desc': 'Total Processes', 'cmd': 'check_procs'},
  'version': {'desc': 'Version', 'cmd': 'check_version'}
}

mapping = {
  'prod-web': ['cpu', 'proc', 'users', 'ssh', 'disk', 'version'],
  'prod-supni': ['cpu', 'proc', 'users', 'ssh', 'log_check', 'disk'],
  'prod-scheduler': ['cpu', 'proc', 'users', 'ssh', 'disk', 'http_81'],
  'prod-dataloader': ['cpu', 'proc', 'users', 'ssh', 'disk', 'http_81'],
  'prod-tramdataloader': ['cpu', 'proc', 'users', 'ssh', 'disk', 'http_81'],
  'prod-nagios': [],
  'default': ['cpu', 'proc', 'users', 'ssh', 'disk', 'http_8081']
}


def tag2name(tag_list):
    """
    Get list of tags on EC2 instance, if tag 'Name' is set, return its value.

    Replace space with underscore. e.g.
    If name is "prod-bus 1a", function returns "prod-bus_1a"
    If tag Name is not found, it returns "Noname"
    """
    if 'Name' in [t['Key'] for t in tag_list]:
        return '_'.join(
            [t for t in tag_list
             if t['Key'] == 'Name'
             ][0]['Value'].split()
        )
    else:
        return 'Noname'


def prod_prefix(instance):
    """Return true if instance Name starts with "prod-" prefix."""
    return any(
        [s.startswith('prod-')
         for s in [t['Value']
                   for t in i.tags if t['Value']
                   ]
         ]
    )


def is_running(instance):
    """Return true if instance is running."""
    return instance.state['Name'] == 'running'


# Get the list of all instances in Ireland region.
all_instances = boto3.resource('ec2', region_name='eu-west-1').instances.all()
instances = [i for i in all_instances]

for i in instances:
    if prod_prefix(i) and is_running(i):
        host_name_ip = tag2name(i.tags) + '_' +\
                        i.private_ip_address.split('.')[-1]
        host_list[host_name_ip] = i.private_ip_address

for key, val in host_list.items():
    host_cfg += host_template % (key, val)

    for service in mapping.get(key.split('_')[0], mapping['default']):
        service_cfg += service_template % (
                                            key, service_list[service]['desc'],
                                            service_list[service]['cmd']
                                          )

shutil.copy2(nagios_hosts_path, nagios_hosts_path + '.orig')
shutil.copy2(nagios_services_path, nagios_services_path + '.orig')

with open(nagios_hosts_path, 'w') as f:
    f.write(host_cfg)
    f.close()

with open(nagios_services_path, 'w') as f:
    f.write(service_cfg)
    f.close()

has_config_changed = False
if not filecmp.cmp(nagios_hosts_path, nagios_hosts_path + '.orig'):
    has_config_changed = True
if not filecmp.cmp(nagios_services_path, nagios_services_path + '.orig'):
    has_config_changed = True

if has_config_changed:
    try:
        print('Checking hosts and services configuration.')
        p = subprocess.Popen(
                             ["service", "nagios", "configtest"],
                             stdout=subprocess.PIPE
                            )
        print('Reloading nagios.')
        subprocess.call(["service", "nagios", "reload"])
        print('Nagios hosts list sucessfully updated!')
    except subprocess.CalledProcessError as exc:
        print "Error occured. Rolling back...\n", exc.returncode, exc.output
        shutil.copy2(nagios_hosts_path + '.orig', nagios_hosts_path)
        shutil.copy2(nagios_services_path + '.orig', nagios_services_path)
else:
    print('No changes detected, nagios not reloaded!')
