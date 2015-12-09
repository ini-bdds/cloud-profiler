from flask import Flask, request, jsonify
import time
import datetime
import os
import ConfigParser
import subprocess
import json

from db_manager import DBManager
from profilerjob.profiler_job import ProfilerJobThread

app = Flask(__name__)

@app.route('/')
def index():
    """
    A simple front page for the service.
    """

    return "Profiling Service."


@app.route('/create-workload', methods=['POST'])
def create_workload():
    """
    An interface to create a new workload and return a directory
    for the client to transfer data to and the workload id as is in the 
    database.
    """
    if request.method == 'POST':
        username = request.form['username']
        secret_key = ""
        access_key = ""
        key_pair = ""
        print "Request from %s" % username
        if 'access_key' in request.form:
            access_key = request.form['access_key']
            secret_key = request.form['secret_key']
            key_pair = request.form['key_pair']
        # Get the user id from the database
        res = {}
        try:

            db_manager = DBManager()
            user_id = db_manager.get_user_id(username, access_key, secret_key,
                                             key_pair)
            # Now create a new workload
            workload_id = db_manager.create_new_workload(user_id)
            workload_dir = create_workload_dir(workload_id)

            # Update the database to reflect the working dir being there
            db_manager.update_workload_dir(workload_id, workload_dir)

            res = {'workload_id': workload_id, 'workload_dir': workload_dir}
        except Exception, e:
            print 'Error: %s' % e
            raise e
        return jsonify(res)


def create_workload_dir(workload_id):
    """
    Create a workload in the database and make a working directory for the
    user to upload their content to.
    """

    config_file = 'profiler.ini'
    # Get the workload directory name from the config
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    base_dir = config.get('Profiler', 'workload_directory')

    working_dir_name = "%sworkload%s" % (base_dir, workload_id)

    # Create the directory
    if not os.path.exists(working_dir_name):
        os.makedirs(working_dir_name)
    submit_line = "chmod 777 %s" % (working_dir_name)
    submit = subprocess.Popen((['sudo','su','root','-c',submit_line]),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
    s_out, s_err = submit.communicate()
    return working_dir_name




@app.route('/request-profiles', methods=['POST'])
def request_profiles():
    """
    Create a profile for the workload as it is run over each of the
    specified instance types.
    """

    config_file = 'profiler.ini'

    if request.method == 'POST':
        description = request.form['description']
        workload = request.form['workload']
        working_dir = request.form['working_dir']
        job_desc = json.loads(description)
        # Create an entry in the db for each job
        db_jobs = {} 
        inst_params = {}
        db_manager = DBManager()


        db_manager.update_workload_dir(workload, working_dir, None)
        try:
            for inst in job_desc['instance_types']:
                instance_type = inst['type']
                job_id = db_manager.insert_job(workload)
                db_jobs.update({instance_type : job_id})
                inst_params.update({instance_type : inst['override']})
        except Exception, e:
            print 'Error with job creation %s' % e
        # While we are at it, set the executable to execuable
        submit_line = "chmod 777 %s%s" % (working_dir, 
                                          job_desc['executable'].split('/')[-1])
        submit = subprocess.Popen((['sudo','su','root','-c',submit_line]),
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
        s_out, s_err = submit.communicate()

        # Now start a thread for each of these jobs
        try:
            for inst, job_id in db_jobs.iteritems():
                instance_type = inst
                params = inst_params[instance_type]
                profiler_thread = ProfilerJobThread(config_file, job_id, 
                                                    inst, params, 
                                                    job_desc)
                profiler_thread.start()
        except Exception, e:
            print 'Error with a thread %s' % e
        res = {'workload' : workload, 'jobs' : db_jobs}
        
        return jsonify(res)


@app.route('/workload-status/<string:val>')
def workload_status(val):
    """
    Return the status of each of the profiles that are being generated 
    for the workload.
    """
    db_manager = DBManager()
    workload_status = "Something went wrong."
    try:
        workload_status = db_manager.get_workload_status(val)
    except Exception, e:
        print e
        raise e
    return jsonify(workload_status)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug = False)

