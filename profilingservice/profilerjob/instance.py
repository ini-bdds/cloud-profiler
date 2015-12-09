from __future__ import with_statement
import boto
import json
import logging
import os
import shutil
import sys
import random
import time
import requests
import psycopg2
import subprocess
import ConfigParser
import tarfile
import csv
from boto.s3.key import Key
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
from distutils.dir_util import copy_tree

from string import Template
from db_manager import DBManager


class Instance():
    """
    This class is used to interact with a profile worker node. The class 
    includes the ability to launch new instance requests and will wait for 
    the resource to be fulfilled. The ability to deploy jobs and collect 
    the resulting profiles is also included in this class.
    """

    def __init__(self, inst_type, subnet, zone, price, db_manager, instances, 
                 address = None, instance_id = 0):
        self.logger = logging.getLogger('profile')
        self.type = inst_type
        self.subnet = subnet
        self.zone = zone
        self.instances = instances
        self.instance_id = instance_id
        self.price = price

        self.address = address
        self.state = None    
        self.status = "Initiated"
        self.exec_id = -1

        for inst in instances:
            if inst.type == inst_type:
                self.cpus = inst.cpus
                self.ami = inst.ami

        self.db_manager = db_manager

        self.load_config()


    def load_config(self):
        """
        Pull all of the config info from the ini file
        """

        config_file = 'profiler.ini'

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        self.domain = config.get('Cloudinit', 'domain')
        self.ip_addr = config.get('Cloudinit', 'ip_addr')
        self.cloudinit_file = config.get('Cloudinit', 'cloudinit_file')

        self.AWSAccessKeyId = config.get('AWS', 'keyid')
        self.AWSSecretKey = config.get('AWS', 'secret')
        self.bucket = config.get('AWS', 'bucket')
        self.key_pair = config.get('AWS', 'key_pair')
        self.security_group = config.get('AWS', 'security_group')


    def customise_cloudinit(self):
        """
        Use a string template to construct an appropriate cloudinit script to
        pass as userdata to the aws request.
        """
        
        d = {'ip_addr': self.ip_addr, 'cpus': self.cpus, 
             'domain': self.domain}
        # Read in the cloudinit file
        filein = open(self.cloudinit_file)
        src = Template(filein.read())
        # Substitute the template fields
        result = src.substitute(d)
        return result


    def tag_requests(self, req, tag, conn):
        """
        Tag any requests that have just been made with the tenant name
        """
        for x in range(0, 3):
            try:
                # Tag the resources as profiling nodes
                conn.create_tags([req], {"tenant": tag})
                conn.create_tags([req], {"Name": 'worker@%s' % tag})
                break
            except boto.exception.BotoClientError as e:
                time.sleep(2)
                pass
            except boto.exception.BotoServerError as e:
                time.sleep(2)
                pass
            except boto.exception.EC2ResponseError:
                logger.exception("There was an error communicating with EC2.")
    
    
    # This code was borrowed from the orignial Mesos project.
    # credit to: http://mesos.apache.org/
    def launch_instance(self, conn):
        """
        Launch a cluster of the given name, by setting up its security groups,
        and then starting new instances in them.
        Returns a tuple of EC2 reservation objects for the master, slave
        Fails if there already instances running in the cluster's groups.
        """
        user_data = self.customise_cloudinit()
        # Request a resource
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
        
        inst_req = conn.request_spot_instances(
            price=self.price, image_id=self.ami,
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



    def wait_for_server(self):
        """
        Wait for the resource to start. We can tell the resource has started
        once the service is running and responsing to requests.
        """
        address = "http://%s:5000/hello" % self.address
        # Check th hello interface to see if it responds
        self.logger.debug(address)
        while True:
            # Only poll every 10 seconds
            time.sleep(10)
            try:
                r = requests.get(address)
                self.logger.debug(r.status_code)
                self.logger.debug(r.text)
                if r.status_code == 200:
                    # Leave once the server is running
                    self.logger.debug("Server is up.")
                    return
            except Exception, e:
                self.logger.debug("Server is not up.")
                self.logger.debug(e)


    def get_status(self):
        """
        Get the status of this instance from the database.
        """
        try:
            r = self.db_manager.get_conn().execute(("select * from instance " +
                                     "where address = '%s';") % (self.address))

            for rec in r:
                self.status = rec["state"]
            self.logger.debug("pulled status from db: %s" % self.status)

        except psycopg2.Error, e:
            self.logger.debug("Failed to get state in the database")
            raise e
        self.logger.debug("State = %s" % self.status)
        return self.status


    def set_status(self, status):
        """
        Set the status of the instance in the database.
        """
        if status == None:
            self.status = "Idle"
        else:
            self.status = status
        self.logger.debug(self.status)
        self.db_manager.set_status(self.status, self.instance_id)


    def set_job_status(self, status, prof_job_id):
        """
        Set the status of the job in the database.
        """
        self.db_manager.update_job_status(status, prof_job_id)


    def get_current_job(self):
        return self.job


    def wait_for_job(self, job_id):
        """
        Periodically poll the service to check when the job is done.
        """
        address = "http://%s:5000/status/%s" % (self.address, job_id)

        self.logger.debug(address)
        while True:
            # Check every 10 seconds if the job is done
            time.sleep(10)
            try:
                r = requests.get(address)
                if r.status_code == 200:
                    # Leave once the job is done
                    self.logger.debug(r.text)
                    if r.text == "4":
                        self.logger.debug("Job finished!")
                        return
                    else:
                        self.logger.debug("Job not finished.")
            except requests.exceptions.RequestException as e:
                self.logger.debug("Getting job status failed.")
                self.logger.debug(e)



    def launch(self):
        """
        Launch the instance and wait for it to initiate and then load.
        """
        self.status = "Starting"
        conn = boto.connect_ec2(self.AWSAccessKeyId, self.AWSSecretKey)

        test = False
        # Launch the instance
        self.launch_instance(conn)
        if self.address == "":
            self.status = "Failed"
            return
        try:
            self.instance_id  = self.db_manager.insert_work_instance(self.type, 
                                                                self.address,
                                                                self.zone, 
                                                                self.price, 
                                                                self.ami)
        except psycopg2.Error, e:
            self.logger.debug("Failed to insert instance into the database")
            raise e

        # Wait for the service to start up
        self.wait_for_server()
        
        self.status = "Idle"
        self.logger.debug("server started")


    def deploy_job(self, job):
        """
        Deploy a job to the service
        """
        self.set_status("Executing")
        self.set_job_status("Profiling", job['id'])
        self.job = job

        # Build a payload to deploy the job to the service
        self.logger.debug(job['executable'])
        address = "http://%s:5000/execute" % self.address
        payload = {"executable" : job['executable'], 
                   "execution" : job['id'],
                   "working_dir" : job['working_dir'],
                   "parameters" : job['params']}
        r = requests.post(address, data=payload)

        self.logger.debug("Made an execute request and got this id:")
        self.logger.debug(r.text)
        job_id = 0
        # Check we got an accept back from the service
        try:
            job_id = int(r.text)
        except Exception, e:
            self.logger.debug("Error getting job_id.")
            self.logger.debug(e)
            return None
        # Wait for the job to finish
        self.wait_for_job(job_id)

        self.set_status("Finished")

        self.store_execution_details(job_id)

        return job_id


    def store_execution_details(self, job_id):
        """
        Store the job details from the execution, e.g. exit status and 
        execution time.
        """

        # Get the execution details from the service
        address = "http://%s:5000/exec-details/%s" % (self.address, job_id)
        try:
            r = requests.get(address)
            if r.status_code == 200:
                # Leave once the job is done
                self.logger.debug(r.text)
                response = json.loads(r.text)
                self.db_manager.store_execution_details(self.job['id'], 
                                                        response)
        except requests.exceptions.RequestException as e:
            self.logger.debug("Getting job status failed.")
            self.logger.debug(e)



    def handle_logs(self):
        """
        Seeing as we have a thread doing all of this other stuff, we might as
        well use it to process the logs as well.
        """
        # Get the file name of the logs
        filename = self.get_job_logs()
        # Since we have our own thread, process the logs in to a csv file
        log_file_name, results = self.process_logs(filename)

        #Store the results in the db so they get sent back to the client
        self.db_manager.store_results(self.job['id'], json.dumps(results))

        # Move the csv log back to the log directory and return that.
        log_dir = "%s/logs/" % (self.job['working_dir'])
        log_dir = log_dir.replace("//", "/")
        log_path = "%s%s" % (log_dir, log_file_name)

        if os.path.exists(log_dir):
            shutil.move(log_file_name, log_dir)

        # Store the results in s3 as well.
        self.store_in_s3(log_file_name, log_path)

        return log_path, results


    def store_in_s3(self, log_file_name, log_path):
        """
        Store the csv log file in s3
        """
        conn = boto.connect_s3(self.AWSAccessKeyId, self.AWSSecretKey)
        #conn = boto.connect_s3(self.access_key, self.secret_key)
        bucket = conn.get_bucket(self.bucket)
        k = Key(bucket)
        k.key = log_file_name
        k.set_contents_from_filename(log_path)

    def get_job_logs(self):
        """
        Copy the logs to the shared file system
        """
        address = "http://%s:5000/logs" % self.address
        payload = {"execution" : self.job['id'],
                   "working_dir" : self.job['working_dir']}

        response = requests.post(address, data=payload)

        res_file = response.text
        try:
            # Untar the file
            tfile = tarfile.open(res_file)
            if tarfile.is_tarfile(res_file):
                tfile.extractall(self.job['working_dir'])
            else:
                self.logger.debug(res_file + " is not a tarfile.")
            # Pull the name of the extracted directory
            filename = res_file.split(".")[0]
            # Get the base name of the logs (without .meta etc.)
            res = "%s/logs/%s" % (self.job['working_dir'], 
                               filename.split("/")[-1])
            res = res.replace("//", "/")
            return res
        except Exception, e:
            print e
            raise e

    def process_logs(self, filename):
        """
        Work through the logs and store the data somewhere.
        """
        #use dumplogs to get the values

        #Add in total cpu fields
        cpu_fields = ['kernel.all.cpu.idle', 'kernel.all.cpu.user',
                  'kernel.all.cpu.sys', 'kernel.all.cpu.wait.total',
                  'kernel.all.cpu.nice', 'kernel.all.cpu.guest',
                  'kernel.all.nprocs']

        #Add in per cpu fields of all of the same data
        per_cpu_fields = ['kernel.percpu.cpu.idle', 'kernel.percpu.cpu.user',
                  'kernel.percpu.cpu.sys', 'kernel.percpu.cpu.wait.total',
                  'kernel.percpu.cpu.nice', 'kernel.percpu.cpu.guest']

        #Add in memory fields
        memory_fields = ['mem.freemem', 'mem.physmem', 'mem.util.bufmem',
                         'mem.util.cached', 'mem.util.commitLimit',
                         'mem.util.committed_AS','mem.util.shmem',
                         'mem.util.slab','mem.util.swapFree',
                         'mem.util.swapTotal','swap.pagesin',
                         'swap.pagesout']


        #Add in network fields - it seems there are two of these, 
        #though one looks to just be the local loopback.
        #either way, add it twice
        network_fields = ['network.interface.in.bytes',
                          'network.interface.in.errors',
                          'network.interface.in.packets',
                          'network.interface.out.bytes',
                          'network.interface.out.errors',
                          'network.interface.out.packets',
                          'network.interface.total.mcasts']
        
        #Add in disk fields
        disk_fields = ['disk.all.read_merge', 'disk.all.read',
                       'disk.all.read_bytes', 'disk.all.write_merge',
                       'disk.all.write', 'disk.all.write_bytes']

        cmd_fields = (cpu_fields + per_cpu_fields + memory_fields +
                     network_fields + disk_fields)


        cmd = ['pmdumptext', '-a', filename]
        cmd = cmd + cmd_fields

        network_name_fields = []
        for x in xrange(0,2):
            for network in network_fields:
                network_name_fields.append("%s%s" % (network, x))
        

        #Set up the name field
        name_fields = ['Time'] + cpu_fields
        for field in per_cpu_fields:
            for i in range(0, self.cpus):
                name_fields.append("%s%s" % (field, i))
        name_fields = (name_fields + memory_fields + 
                       network_name_fields + disk_fields)

        # Give the output csv a nice name
        params = "".join(self.job['params']).replace('$' , '-')
        csv_file_name = '%s%s-%s%s.csv' % (self.job['workload_name'], 
                                           self.job['id'], 
                                           self.job['inst_type'], params)
        with open(csv_file_name , 'wb') as fp:
            csvfile = csv.writer(fp, delimiter=',')
            csvfile.writerows([name_fields])

            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            while True:
              line = proc.stdout.readline()
              line = line.strip()
              if line != '':
                #the real code does filtering here
                data = line.split('\t')
                csvfile.writerows([data])
              else:
                break
        
        # While we are here in this thread we might as well process the csv
        # to create a response doc of cpu, memory, network, and disk usage
        results = self.get_results(csv_file_name)

        return csv_file_name, results

    def get_results(self, csv_file):
        """
        Process the csv file to construct a results structure which can
        be returned to the client.
        """

        cr = csv.reader(open(csv_file,"rb"))
        head = cr.next() # to skip the header 

        indices = {"kernel.all.cpu.idle" : head.index("kernel.all.cpu.idle"),
            "kernel.all.cpu.user" : head.index("kernel.all.cpu.user"),
            "kernel.all.cpu.sys" : head.index("kernel.all.cpu.sys"),
            "mem.freemem" : head.index("mem.freemem"),
            "mem.physmem" : head.index("mem.physmem"),
            "disk.all.write_bytes" : head.index("disk.all.write_bytes"),
            "disk.all.read_bytes" : head.index("disk.all.read_bytes"),
            "network.interface.in.bytes0" : head.index("network.interface.in.bytes0"),
            "network.interface.in.bytes1" : head.index("network.interface.in.bytes1"),
            "network.interface.out.bytes0" : head.index("network.interface.out.bytes0"),
            "network.interface.out.bytes1" : head.index("network.interface.out.bytes1")}

        totals = {"kernel.all.cpu.idle" : 0,
                  "kernel.all.cpu.user" : 0,
                  "kernel.all.cpu.sys" : 0,
                  "mem.freemem" : 0,
                  "mem.physmem" : 0,
                  "disk.all.write_bytes" : 0,
                  "disk.all.read_bytes" : 0,
                  "network.interface.in.bytes0" : 0,
                  "network.interface.in.bytes1" : 0,
                  "network.interface.out.bytes0" : 0,
                  "network.interface.out.bytes1" : 0}

        # A list of which ones should be averaged
        divide = ["kernel.all.cpu.idle", "kernel.all.cpu.user", 
                  "kernel.all.cpu.sys", "mem.freemem"]
        count = 0
        for row in cr:
            if '?' not in row:
                count += 1
                for name, index in indices.iteritems():
                    totals[name] += float(row[index])
                totals['mem.physmem'] = float(row[indices['mem.physmem']])

        for name in divide:
            totals[name] = totals[name] / count

        cpu = {'Avg_Idle' : totals["kernel.all.cpu.idle"],
               'Avg_User' : totals["kernel.all.cpu.user"],
               'Avg_Sys' : totals["kernel.all.cpu.sys"]}
        memory = {'Avg_Free' : totals["mem.freemem"],
                  'Phys_Mem' : totals["mem.physmem"]}
        disk = {'Write_Bytes' : totals["disk.all.write_bytes"],
                'Read_Bytes' : totals["disk.all.read_bytes"]}
        network = {'Total_In' : (totals['network.interface.in.bytes0'] + 
                                 totals['network.interface.in.bytes1']),
                   'Total_Out' : (totals['network.interface.out.bytes0'] + 
                                 totals['network.interface.out.bytes1'])}
        results = {'cpu' : cpu, 'memory' : memory, 'disk' : disk, 
                   'network' : network}
        return results

    def get_job_stats(self, job_id):
        address = "http://%s:5000/stats/%s" % (self.address, job_id)
        res = "job:%s" % job_id
        try:
            r = requests.get(address)

            if r.status_code == 200:
                self.logger.debug(r.text)
                info = r.text.split(",")
                for i in info:
                    vals = i.split(":")
                    if len(vals) > 1:
                        res = "%s,%s:%s" % (res, vals[0],vals[1])
                        self.logger.debug("Job %s, %s = %s" % (job_id, vals[0],
                                     vals[1]))
            return res
        except requests.exceptions.RequestException as e:
            self.logger.debug("Getting job stats failed.")
            self.logger.debug(e)

        return res
