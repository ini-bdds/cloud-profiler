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

import requests


class Client(object):

    def __init__(self):
        self.load_config()


    def load_config(self):
        """
        Pull all of the config info from the ini file
        """
        config_file = 'client.ini'

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        self.access_key = config.get('AWS', 'keyid')
        self.secret_key = config.get('AWS', 'secret')
        self.key_pair = config.get('AWS', 'key_pair')

        self.username = config.get('Client', 'username')
        self.ssh_key = config.get('Client', 'ssh_key')

        # self.input_dir = config.get('Profile', 'input_dir')
        # self.executable = config.get('Profile', 'executable')
        # self.instances = config.get('Profile', 'instances')
        # self.name = config.get('Profile', 'name')


        self.profiler_address = config.get('Profiler', 'address')
        self.profiler_port = config.get('Profiler', 'port')


        self.job_file = config.get('Job', 'description')
        with open(self.job_file) as data_file:    
            self.job_desc = json.load(data_file)


    def run(self):
        """
        Run the client application. This should just communicate
        with the profiling service and organise itself in to profiling
        a tool on a couple of different instance types.
        """

        print self.job_desc

        sys.exit(0)

        print "Creating profiling dir"
        # Create a workload request
        profiling_dir = self.create_workload()

        print "Transferring files"
        # Transfer the input directory to the profiler
        self.transfer_files(profiling_dir)

        print "Requesting profiles"
        # Once the transfer completes request the workload is profiles
        self.request_profiles(profiling_dir)

        # Now wait for the workload status to be complete
        while True:
            status = self.get_workload_status()
            print status
            if 'Complete' in status:
                break
            time.sleep(10)



    def get_workload_status(self):
        """
        Check the status of the workload
        """

        address = "http://%s:%s/workload-status/%s" % (self.profiler_address,
                                                       self.profiler_port,
                                                       self.workload_id)

        try:
            r = requests.get(address)
            return r.text
        except requests.exceptions as e:
            print "Error communicating with the profiler: %s" % e


    def request_profiles(self, profiling_dir):
        """
        Create a request to the profiler to begin profiling the workload.
        """

        address = "http://%s:%s/request-profiles" % (self.profiler_address,
                                                    self.profiler_port)

        # bit of a workaround. It wouldn't let me transfer just the contents
        # of the directory, so lets just change the working dir
        working_dir = '%s/%s/' % (profiling_dir, self.input_dir.split('/')[-2])

        payload = {'workload' : self.workload_id,
                   'working_dir' : working_dir,
                   'description' : self.job_desc}
        try:
            r = requests.post(address, payload)
            print "Received: %s from the profiler" % r.text
        except requests.exceptions as e:
            print "Error communicating with the profiler: %s" % e


    def create_workload(self):
        """
        Start by creating a new workload with the profiling service.
        Pass in a my username and get the working directory from the service
        """
        address = "http://%s:%s/create-workload" % (self.profiler_address,
                                                    self.profiler_port)
        payload = {'username': self.username, 'access_key': self.access_key,
                   'secret_key': self.secret_key, 'key_pair': self.key_pair,
                   'executable' : self.executable}
        print payload
        try:
            r = requests.post(address, payload)
            print "Received: %s from the profiler" % r.text
            res = json.loads(r.text)
            self.workload_id = res['workload_id']
            return res['workload_dir']
        except requests.exceptions as e:
            print "Error communicating with the profiler: %s" % e


    def transfer_files(self, dest_dir):
        """
        Transfer the input directory contents to the working dir
        """

        dest = 'ubuntu@%s:%s/' % (self.profiler_address, dest_dir)
        ssh_key = ""
        if os.path.exists(self.input_dir):
            cmd = ['scp']
            if self.ssh_key is not None:
            	cmd.append('-i')
            	cmd.append(self.ssh_key)
            cmd.append('-r')
            cmd.append('%s' % self.input_dir)
            cmd.append(dest)
            cmd_str = " ".join(cmd)
            print cmd_str
            try:
	            #cmd = ['sudo', 'su', 'galaxy', '-c', cmd_str]
                out = subprocess.Popen(cmd, stdout=subprocess.PIPE)
                res = out.wait()
            except Exception, e:
                print 'error %s' % e
                raise e


if __name__ == '__main__':

    client = Client()
    client.run()

