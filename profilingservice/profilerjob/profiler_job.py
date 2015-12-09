#!/usr/bin/env python

import sys
import os
import boto
import time
import math
import subprocess
import optparse
import logging
import threading
import psycopg2
import sqlalchemy
import ConfigParser
import datetime
import json

from threading import Thread
from pytz import timezone
from db_manager import DBManager
from resource import Resource
from instance import Instance

class ProfilerJobThread(Thread):

    def __init__(self, config_file, job_id, inst_type, params, job_desc):
        try:
            self.load_config(config_file)
            self.job_id = job_id
            self.inst_type = inst_type
            self.logger = logging.getLogger('profile')
            self.params = params
            self.workload_name = job_desc['name']
            self.db_manager = DBManager()
            self.job_desc = job_desc
            for inst in self.job_desc['instance_types']:
                if inst['type'] == self.inst_type:
                    self.inst_desc = inst
                    break
        except Exception, e:
            print e
        try:
            if not self.logger.handlers:
                hdlr = logging.FileHandler('/home/ubuntu' +
                                           '/profiler/profiles.log')
                fm = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
                hdlr.setFormatter(fm)
                self.logger.addHandler(hdlr)
                self.logger.setLevel(logging.DEBUG)
                consoleHandler = logging.StreamHandler()
                consoleHandler.setFormatter(fm)
                self.logger.addHandler(consoleHandler)

        except Exception, e:
            print e
        Thread.__init__(self)


    def load_config(self, config_file):
        """
        Pull all of the config info from the ini file
        """
        cloudinit_file = "cloudinit.cfg"

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        self.AWSAccessKeyId = config.get('AWS', 'keyid')
        self.AWSSecretKey = config.get('AWS', 'secret')

    def run(self):
        """
        Run the profiler_job thread. This should handle the whole process
        of managing an aws instance, getting the workload executed, collecting
        stats and retrieving the profile.
        """
        # Start by finding or creating an instance to run on. We have
        # the job_id, so get all of the job information from the database
        job_info = self.get_job_info()

        # Now get any existing aws instances
        instances = self.get_existing_instances()

        # Check if one of them will fulfil the job
        worker = self.get_worker_instance(instances, self.inst_type)

        # Now we have the worker instance we are going to use, so firstly
        # update the database to reflect the work_instance
        self.db_manager.update_job(self.job_id, worker.instance_id)
        self.db_manager.update_job_status("Deploying", self.job_id)

        # Now we need to deploy the job
        worker.deploy_job(job_info)
        self.db_manager.update_job_status("Processing Logs", self.job_id)


        # Retrieve the logs and put them in the shared FS
        csv_log = worker.handle_logs()
        self.db_manager.update_job_status("Complete", self.job_id)

        return csv_log

    def get_job_info(self):
        """
        Get the job information from the database. This should say what
        instance type is required and what the execuable is etc.
        """
        try:
            rows = self.db_manager.get_conn().execute((
                "select profile_job.id as pid, profile_job.workload_id, " +
                "profile_job.work_instance_id, profile_job.execution_time, " +
                "profile_job.exit_status, profile_job.status, " +
                "workload.id as wid, " +
                "workload.working_dir, workload.client_id " +
                "from profile_job, workload where profile_job.id = %s and " +
                "workload.id = profile_job.workload_id") % self.job_id)
            for row in rows:
                info = {'id' : row['pid'], 
                        'workload_id' :  row['workload_id'],
                        'work_instance_id' :  row['work_instance_id'],
                        'execution_time' :  row['execution_time'],
                        'exit_status' :  row['exit_status'],
                        'status' :  row['status'],
                        'working_dir' :  row['working_dir'],
                        'client_id' :  row['client_id'],
                        'executable' :  self.job_desc['executable'],
                        'params' : self.params,
                        'inst_type' : self.inst_type,
                        'workload_name' : self.workload_name}
                return info
        except psycopg2.Error:
            self.logger.exception("Error getting inst types from database.")
        self.logger.debug("The set of instances from the database:")


    def get_existing_instances(self):
        """
        Find any existing instances on this connection and add them to the 
        list.
        """
        conn = boto.connect_ec2(self.AWSAccessKeyId, self.AWSSecretKey)

        reservations = conn.get_all_instances()
        instance_types = self.load_instances()
        aws_instances = [i for r in reservations for i in r.instances]

        instances = []
        try:
            for i in aws_instances:
                records = self.db_manager.get_conn().execute(
                    ("SELECT work_instance.id as wid, address, zone, price, " +
                     "instance_type.type FROM work_instance, instance_type " +
                     "where address = '%s' and work_instance.type = " +
                     "instance_type.id") % i.private_dns_name)

                for rec in records:
                    new_inst = Instance(rec['type'], '', rec['zone'],
                                        rec['price'],
                                        self.db_manager, instance_types, 
                                        i.private_dns_name, rec['wid'])
                    instances.append(new_inst)
        except psycopg2.Error, e:
            self.logger.error("Failed to get instance from database")
            raise e

        return instances


    def get_worker_instance(self, instances, inst_type):
        """
        Work out if there is already an existing instance that will fulfill
        the job. If there is not, then a new instance should be requested.
        Because this thread is running by itself, we can wait for the
        instance to launch and be set up before returning.
        """

        # Check if one of the existing resources will do the job
        for inst in instances:
            if inst.type == inst_type:
                return inst

        # Otherwise acquire a new instance

        self.logger.debug("no istances found, so starting a new one.")
        #no instances of this type exist, so start one
        zone = self.get_cheapest_spot_zone(inst_type)
        subnet = self.get_subnet_id(zone)
        cpus = 0
        instance_types = self.load_instances()
        for ins in instance_types:
            if ins.type == inst_type:
                cpus = ins.cpus
                break
        # Create an instance object from this data
        new_inst = Instance(inst_type, subnet, zone, 
                            self.inst_desc['bid'], self.db_manager, 
                            instance_types)


        # Now launch the instance and wait for it to come up
        new_inst.launch()
        return new_inst


    def load_instances(self):
        """
        Get the set of instances from the database
        """
        instance_types = []
        try:
            rows = self.db_manager.get_conn().execute(
                "select * from instance_type")
            for row in rows:
                instance_types.append(Resource(
                    row['id'], row['type'], row['ondemand_price'],
                    row['cpus'], row['memory'], row['disk'], row['ami']))
        except psycopg2.Error:
            self.logger.exception("Error getting instance types " +
                                  "from database.")
        self.logger.debug("The set of instances from the database:")
        self.logger.debug(instance_types)

        return instance_types


    def start_job_handler(self, job):
        """
        Start a thread to handle a job's execution.
        """
        #first find an instance for this job
        self.logger.debug("thread started")
        inst = self.get_available_instance(job['instance_type'])
        job_id = 0
        while inst.get_status() != "Idle":
            self.logger.debug("waiting for idle")
            time.sleep(10)
        if inst.get_status() == "Idle":
            result = inst.deploy_job(job)
            self.logger.debug("result = %s" % result)
            try:
                if result != None:
                    job_id = int(result)
                else:
                    "Failed to deploy job."
                    return
            except Exception, e:
                self.logger.error("Error: no job id from deploy_job()")
                self.logger.error(e)


        while inst.get_status() != "Finished":
            time.sleep(10)

        #Now get the logs and start to process them
        inst.handle_logs()

        inst.set_status("Idle")

        stats = inst.get_job_stats(job_id)

        self.logger.debug(stats)


    def get_cheapest_spot_zone(self, inst_type):
        """
        Select the cheapest instance zone to launch in.
        """
        utc = timezone('UTC')
        utc_time = datetime.datetime.now(utc)
        now = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        conn = boto.connect_ec2(self.AWSAccessKeyId, self.AWSSecretKey)
        timeStr = str(now).replace(" ", "T") + "Z"

        prices = conn.get_spot_price_history(instance_type=inst_type, 
                                             product_description="Linux/UNIX", 
                                             start_time=timeStr, 
                                             end_time=timeStr)
        inst_zone = ""
        min_price = float('inf')
        for price in prices:
            if price.price < min_price:
                inst_zone = price.availability_zone
        self.logger.debug("selecting %s" % inst_zone)

        return inst_zone


    def get_subnet_id(self, zone):
        """
        Now get the subnet details
        I can't seem to find a great example of how to do this with boto.
        The examples seem to create a VPC connection, which I don't think
        is what we want. I also can't seem to find them in the meta data.
        All we want to do is use 'aws ec2 describe-subnets', so bugger it,
        lets just do that.
        """

        cmd = ['aws', 'ec2', 'describe-subnets']
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
        json_res = json.loads(out)

        for key, val in json_res.iteritems():
            for vpc in val:
                # Check if 'worker-subnet-*' is in the tags. Otherwise it will
                # capture the headnode subnets as well.
                #if 'worker-subnet' in vpc['Tags'][0]['Value']:
                if '172.30' in vpc['CidrBlock']:
                    if zone == vpc['AvailabilityZone']:
                        # Should this return SubnetId?
                        return vpc['SubnetId']

