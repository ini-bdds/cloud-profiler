import time
import datetime
import os
import ConfigParser
import psycopg2
import sqlalchemy

class DBManager(object):

    def __init__(self):
        self.config_file = 'profiler.ini'
        self.load_config()


    def load_config(self):
        """
        Create a db connection from the config file
        """

        # read config from a file
        config = ConfigParser.ConfigParser()
        config.read(self.config_file)

        # get DB connection info
        user = config.get('Database', 'user')
        password = config.get('Database', 'password')
        host = config.get('Database', 'host')
        port = config.get('Database', 'port')
        database = config.get('Database', 'database')

        #aws stuff
        self.access_key = config.get('AWS', 'keyid')
        self.secret_key = config.get('AWS', 'secret')
        self.bucket = config.get('AWS', 'bucket')

        # create a connection and keep it as a config attribute
        try:
            engine = sqlalchemy.create_engine(
                'postgresql://%s:%s@%s:%s/%s' %
                (user, password, host, port, database))
            conn = engine.connect()
            self.dbconn = conn
        except psycopg2.Error:
            logger.exception("Failed to connect to database.")


    def get_user_id(self, username, access_key, secret_key, key_pair):
        """
        Connect to the database and eiter insert or get the db id of the user.
        """

        # Check if the user already exists in the database
        cmd = "SELECT id FROM client WHERE username = '%s'" % username

        try:
            r = self.dbconn.execute(cmd)
            for rec in r:
                return rec['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to get client from db")
            raise e

        # If we got here, then the client does not exist, so insert it

        insert_aws = (("INSERT INTO aws_credentials (access_key, " +
                       "secret_key, key_pair) VALUES ('%s','%s','%s') " +
                       "RETURNING id") % (access_key, secret_key, key_pair))
        aws_cred = 0
        try:
            r = self.dbconn.execute(insert_aws)
            for rec in r:
                aws_cred = int(rec['id'])
        except psycopg2.Error, e:
            self.logger.debug("Failed to insert aws credentials")
            raise e

        if aws_cred == 0:
            self.logger.debug('Failed to get aws cred id')

        # Now insert the new client

        insert_client = (("INSERT INTO client (username, " +
                          "aws_credentials_id) " + 
                          "VALUES ('%s',%s) RETURNING id")
                          % (username, aws_cred))
        
        try:
            r = self.dbconn.execute(insert_client)
            for rec in r:
                return int(rec['id'])
        except psycopg2.Error, e:
            self.logger.debug("Failed to insert new client")
            raise e


    def create_new_workload(self, user_id):
        """
        Insert a new workload in the database
        """
        cmd = ("INSERT INTO workload (client_id, executable, " + 
                      "working_dir) VALUES (%s,'','') RETURNING " + 
                      "id") % (user_id)

        workload_id = 0
        try:
            r = self.dbconn.execute(cmd)
            for rec in r:
                workload_id = rec['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to get client from db")
            raise e
        return workload_id


    def update_workload_dir(self, work_id, working_dir, executable):
        """
        Upload the workload to include the working working_dir
        """

        cmd = ("UPDATE workload set working_dir = '%s', executable = " +
               "'%s' where id = %s") % (working_dir, executable, work_id)
        if executable == None:
            cmd = ("UPDATE workload set working_dir = '%s' " +
                   "where id = %s") % (working_dir, work_id)


        workload_id = 0
        try:
            r = self.dbconn.execute(cmd)
        except psycopg2.Error, e:
            self.logger.debug("Failed to update workload directory")
            raise e


    def get_workload_status(self, workload_id):
        """
        Get the status of each profile job that is being run for this
        workload
        """
        
        cmd = ("SELECT profile_job.execution_time, profile_job.exit_status," +
               "profile_job.status, instance_type.type from profile_job, " +
               "work_instance, instance_type where workload_id = %s and " +
               "profile_job.work_instance_id = work_instance.id and " +
               "instance_type.id = work_instance.type") % (workload_id)
        print cmd
        profile_res = []
        try:
            r = self.dbconn.execute(cmd)

            for res in r:
                profile_res.append({'instance' : res['type'],
                                    'status' : res['status'],
                                    'exec_time' : res['execution_time'],
                                    'exit_status' : res['exit_status']})
                print res
        except psycopg2.Error, e:
            print e
            self.logger.debug("Failed to update workload directory")
            raise e


        response = {'workload' : workload_id, 'profiles' : profile_res}
        print response
        return response

    def insert_job(self, workload_id):
        """
        Insert a new workload in the database. This is just a temporary
        place holder until the instance is started. Then this will need to
        be updated to reflect the information from the worker instance.
        """

        cmd = ("INSERT INTO profile_job (workload_id, status)" + 
               " VALUES " +
               "(%s, 'Created') RETURNING id") % (workload_id)

        job_id = 0
        try:
            r = self.dbconn.execute(cmd)
            for rec in r:
                job_id = rec['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to get client from db")
            raise e
        return job_id

    def update_job(self, job_id, work_inst_id):
        """
        Update the job record to include details of the worker instance
        """
        print 'Updating profile job with work instance id.'
        cmd = ("UPDATE profile_job set work_instance_id = %s, status = " +
               "'Assigned' where id = %s RETURNING id") % (work_inst_id, 
                                                           job_id)

        job_id = 0
        try:
            r = self.dbconn.execute(cmd)
            for rec in r:
                job_id = rec['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to get client from db")
            raise e
        return job_id
        

    def insert_work_instance(self, inst_type, address, zone, price, ami):
        """
        Insert a new work instance in to the table and return the id.
        """
        # First get the id of the instance type
        inst_id = self.get_instance_type_id(inst_type)
        try:
            # Insert instance details in to the database
            cmd = ("INSERT INTO work_instance (type, address, zone, price," +
                   " ami) values ('%s', '%s', '%s', '%s', '%s') " + 
                   "RETURNING id") % (inst_id, address, zone, price, ami)
            rows = self.dbconn.execute(cmd)
            for row in rows:
                return row['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to insert instance into the database")
            raise e

    def set_status(self, status, instance_id):
        """
        Update the status of the instance.
        """
        try:
            # Insert instance details in to the database
            cmd = ("UPDATE work_instance set state = '%s' where id = " +
                   "%s") % (status, instance_id)
            self.dbconn.execute(cmd)
        except psycopg2.Error, e:
            self.logger.debug("Failed to update instance state in db")
            raise e


    def get_instance_type_id(self, inst_type):
        """
        Retrive the id from the database of the instance type with this name
        """

        cmd = "SELECT id FROM instance_type WHERE type = '%s'" % inst_type

        try:
            r = self.dbconn.execute(cmd)
            for rec in r:
                return rec['id']
        except psycopg2.Error, e:
            self.logger.debug("Failed to get client from db")
            raise e


    def update_job_status(self, status, job_id):
        """
        Update the status of a profile job in the database
        """
        try:
            # Insert instance details in to the database
            cmd = ("UPDATE profile_job set status = '%s' where " +
                   "id = %s") % (status, job_id)
            self.dbconn.execute(cmd)
        except psycopg2.Error, e:
            self.logger.debug("Failed to update job status in db")
            raise e


    def store_execution_details(self, job_id, details):
        """
        Update the job record to include details of execution, e.g.
        the time to execute and the exit status.
        """

        cmd = ("UPDATE profile_job set execution_time = %s, exit_status = " +
               "'%s' where id = %s") % (details['execution_time'],
                                        details['exit_status'], job_id)

        try:
            r = self.dbconn.execute(cmd)
        except psycopg2.Error, e:
            self.logger.debug("Failed to update exec details in db")
            raise e

    def get_conn(self):
        return self.dbconn


    def close_conn(self):
        self.dbconn.close()

    # def get_job_status(self, job_id):
    #     """
    #     Get the status of a profile job in the database
    #     """
    #     try:
    #         # Insert instance details in to the database
    #         cmd = ("SELECT * from profile_job where " +
    #                "id = %s") % (status, job_id)
    #         res = self.dbconn.execute(cmd)
    #         for row in res:
    #             return row['status']
    #     except psycopg2.Error, e:
    #         self.logger.debug("Failed to update job status in db")
    #         raise e
