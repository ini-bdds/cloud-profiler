#!/usr/bin/env python

import sys
import os
import time
import math
import optparse
import logging
import ConfigParser
import datetime
import json
import subprocess
import psycopg2
import sqlalchemy
import boto
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
import requests


class Deploy(object):

    def __init__(self):
        self.load_config()


    def load_config(self):
        """
        Pull all of the config info from the ini file
        """
        config_file = 'deploy.ini'

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        self.access_key = config.get('AWS', 'keyid')
        self.secret_key = config.get('AWS', 'secret')
        self.key_pair = config.get('AWS', 'key_pair')

        self.security_group = config.get('Instance', 'security_group')
        self.type = config.get('Instance', 'type')
        self.bid = config.get('Instance', 'bid')
        self.ami = config.get('Instance', 'ami')
        self.subnet = config.get('Instance', 'subnet')
        self.ondemand = False
        if self.bid == 'ondemand':
            self.ondemand = True

        # get DB connection info
        user = config.get('Database', 'user')
        password = config.get('Database', 'password')
        host = config.get('Database', 'host')
        port = config.get('Database', 'port')
        database = config.get('Database', 'database')

        # create a connection and keep it as a config attribute
        try:
            engine = sqlalchemy.create_engine(
                'postgresql://%s:%s@%s:%s/%s' %
                (user, password, host, port, database))
            conn = engine.connect()
            self.dbconn = conn
        except psycopg2.Error:
            logger.exception("Failed to connect to database.")


    def run(self):
        """
        Automatically deploy the profiling system. This should launch a new
        instance for the profiler and use cloudinit to set it up, then connect
        to the database and make sure it is properly set up as well.
        """

        # Start by launching an instance for the profiler
        self.launch_instance()

        # Now make sure the database is properly set up
        #self.configure_database()


    def configure_database(self):
        """
        Execute the .sql file and ensure all of the necessary tables exist in
        the database.
        """

        with self.dbconn as cursor:
            cursor.execute(open("create_db.sql", "r").read())


    def launch_instance(self):
        """
        Launch an instance for the profiler to run on.
        """

        # Read in the cloudinit file
        cfgfile = open('deploy.cfg')
        user_data = cfgfile.read()

        # Create a block mapping to get instance storage for the machine
        mapping = BlockDeviceMapping()
        sda1 = BlockDeviceType()
        eph0 = BlockDeviceType()
        eph1 = BlockDeviceType()
        eph2 = BlockDeviceType()
        eph3 = BlockDeviceType()
        sda1.size = 10
        eph0.ephemeral_name = 'ephemeral0'
        eph1.ephemeral_name = 'ephemeral1'
        eph2.ephemeral_name = 'ephemeral2'
        eph3.ephemeral_name = 'ephemeral3'
        mapping['/dev/sda1'] = sda1
        mapping['/dev/sdb'] = eph0
        mapping['/dev/sdc'] = eph1
        mapping['/dev/sdd'] = eph2
        mapping['/dev/sde'] = eph3

        conn = boto.connect_ec2(self.access_key, self.secret_key)
        
        inst_req = None

        if self.ondemand:
            inst_req= conn.run_instances(
            min_count=1, max_count=1,
            key_name=self.key_pair, image_id=self.ami,
            security_group_ids=[self.security_group],
            user_data=user_data,
            instance_type=self.instance_type,
            block_device_map=mapping)
        
        else:
            inst_req = conn.request_spot_instances(
            price=self.bid, image_id=self.ami,
            subnet_id=self.subnet, count=1,
            key_name=self.key_pair,
            security_group_ids=[self.security_group],
            instance_type=self.type,
            user_data=user_data,
            block_device_map=mapping)

        # Reorder the list of id's
        my_req_ids = [req.id for req in inst_req]
        # Tag the request
        for req in my_req_ids:
            self.tag_requests(req, 'profiler', conn)
        
        self.address = ""
        # Wait for the resource to become active
        while self.address == "":
            while True:
                # Only check every 10 seconds
                time.sleep(10)
                # Get all spot requests
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                    active = 0
                    instance_ids = []
                for i in my_req_ids:
                    try:
                        # Once it is active add it to the list
                        if id_to_req[i].state == "active":
                            active += 1
                            instance_ids.append(id_to_req[i].instance_id)
                    except Exception, e:
                        self.logger.debug("Hmm, error, skipping for now.")
                if active == 1:
                    # Once it is granted we can leave this loop
                    self.logger.debug("All %d slaves granted" % 1)
                    reservations = conn.get_all_instances(instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break

            self.address = slave_nodes[0].private_dns_name
        # Tag the operating instance
        self.tag_requests(slave_nodes[0].id, 'profiler', conn)


    def tag_requests(self, req, tag, conn):
        """
        Tag any requests that have just been made with the tenant name
        """
        for x in range(0, 3):
            try:
                # Tag the resources as profiling nodes
                conn.create_tags([req], {"Name": tag})
                break
            except boto.exception.BotoClientError as e:
                time.sleep(2)
                pass
            except boto.exception.BotoServerError as e:
                time.sleep(2)
                pass
            except boto.exception.EC2ResponseError:
                logger.exception("There was an error communicating with EC2.")



if __name__ == '__main__':

    deploy = Deploy()
    deploy.run()

