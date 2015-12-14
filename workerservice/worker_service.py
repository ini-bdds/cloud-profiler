from flask import Flask, request, jsonify
import subprocess
import time
import re
import psutil
from random import randint
import datetime
import os
from string import Template
import tarfile
from distutils.dir_util import copy_tree
app = Flask(__name__)

@app.route('/')
def index():
    return 'Index Page'

#testing
@app.route('/hello')
def hello_world():
    return "test %s" % 'Hello World'

#check job status - return the condor code for this job.
@app.route('/status/<string:val>')
def get_status(val):
    cmd = [ 'condor_history', '-format', '%s', 'JobStatus',
            '-constraint', 'ClusterId == %s' % val ]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    if float(output) >= 0:
        if output == '4':
            #The job is done, so stop the logger
            terminate_process("pmlogger")
        return output
    else:
        return "Job %s not in history" % val



@app.route('/docker_status/<string:val>')
def get_docker_status(val):
"""
check job status - return the true or false if the job is running
"""
    cmd = ['docker', 'inspect', '-f', '{{.State.Running}}', val]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    if float(output) >= 0:
        if output == 'false':
            #The job is done, so stop the logger
            terminate_process("pmlogger")
            return 'Finished'
        else:
            return 'Running'
    else:
        return "Job %s not in history" % val


@app.route('/exec-details/<string:val>')
def exec_details(val):

    status = {
             0 : 'Unexpanded',
             1 : 'Idle',
             2 : 'Running',
             3 : 'Removed',
             4 : 'Completed',
             5 : 'Held',
             6 : 'Submission_err'
             }
    cmd = [ 'condor_history', '-format', '%s,', 'JobStatus', '-format', '%s',
            'RemoteWallClockTime', '-constraint', 'ClusterId == %s' % val ]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    res = {}

    exit_status = status[int(output.split(',')[0])]
    res = {'exit_status' : exit_status,
           'execution_time' : output.split(',')[1]}
    return jsonify(res)




@app.route('/logs', methods=['POST'])
def get_logs():

    try:
        if request.method == 'POST':
            execution = request.form['execution']
            working_dir = request.form['working_dir']
            # The location of log files locally
            log_files = "/home/ubuntu/logs/Job-%s/" % execution
            if os.path.exists(working_dir):
                filename = "%s/Job-%s.tar" % (working_dir, execution)
                with tarfile.open(filename, "w:gz") as tar:
                    print log_files
                    tar.add(log_files, arcname='logs')
                return filename
    except Exception, e:
        print e
        raise e


#get the monitored stats on the job.
@app.route('/stats/<string:val>')
def get_stats(val):
    cmd = [ 'condor_history', '-format', 'id:%s,', 'ClusterId',
            '-format', 'status:%s,', 'JobStatus','-format', 
            'time:%s', 'RemoteWallClockTime', '-constraint', 
            'ClusterId == %s' % val ]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    if val in output:
        return output
    else:
        return "Job %s not in history" % val

def create_directories(working_dir):
    directory = "/ephemeral/0/working-%s" % working_dir
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory



def create_submission_files(execution, directory, executable):
    """
    Create the submission file for condor.
    """
    ofile = "%s/test%s.out" % (directory, execution)
    efile = "%s/test%s.err" % (directory, execution)
    user_log = "%s/test%s.log" % (directory, execution)
    submit_desc = [ ]
    submit_desc.append( 'universe = vanilla' )
    submit_desc.append( 'getenv = true' )
    submit_desc.append( 'executable = ' + executable)
    submit_desc.append( 'output = ' + ofile )
    submit_desc.append( 'error = ' + efile )
    submit_desc.append( 'log = ' + user_log )
    submit_desc.append( 'notification = NEVER' )
    submit_desc.append( 'queue' )
    submit_file = "%s/test%s.submit" % (directory, execution)
    try:
        fh = file( submit_file, 'w' )
        for line in submit_desc:
            fh.write( line + '\n' )
        fh.close()
    except Exception, e:
        print "Failed to create submition file."
        raise e
    return submit_file

def modify_exec_file(execution, directory, executable, params):
    """
    Modify the executable to cd to the home directory before executing.
    All this needs to do is cd as the second line.
    """

    f = open(executable, "r")
    contents = f.readlines()
    f.close()

    contents.insert(1, "cd %s\n" % directory)

    f = open(executable, "w")
    contents = "".join(contents)

    # Now we want to replace any parameters which are in a string
    # format of $abc=4;$xyz='hello'. So, first check if theres anything
    # to do, then split that string out and create a dict of it
    if params != "":
        param_dict = {}
        param_list = params.split(";")
        param_list.append('$home=%s' % directory)
        print param_list
        for p in param_list:
            contents = contents.replace(p.split('=')[0], p.split('=')[1])
    print contents
    f.write(contents)
    f.close()


def submit_job(submit_file):
    job_id = None
    try:
        submit_line = "condor_submit %s" % submit_file
        submit = subprocess.Popen((['sudo','su','ubuntu','-c',submit_line]),
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT)
        s_out, s_err = submit.communicate()
        if submit.returncode == 0:
            match = re.search( 'submitted to cluster (\\d+).', s_out )
            if match is None:
                s_out = 'Failed to find job id from condor_submit'
            else:
                job_id = match.group( 1 )
        return job_id
    except Exception, e:
        print e
        raise e

def submit_docker_job(executable, execution):
    job_id = None
    try:
        submit_line = "docker run --name docker_%s %s" % (execution, executable)
        submit = subprocess.Popen((['sudo','su','ubuntu','-c',submit_line]),
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.STDOUT)
        s_out, s_err = submit.communicate()
        if submit.returncode == 0:
            # Just grab the output, whihc is hopefully a string id
            match = s_out
            if match is None:
                s_out = 'Failed to find job id from docker run'
            else:
                job_id = match
        return job_id
    except Exception, e:
        print e
        raise e

def copy_working_dir_contents(working_dir, directory):
    """
    Copy the cotents of the working diretory to the newly
    created directory for local execution.
    """
    copy_tree(working_dir, directory)

#add a job to the condor queue
@app.route('/execute', methods=['POST'])
def handle_request():
    try:

        if request.method == 'POST':
            executable = request.form['executable']
            execution = request.form['execution']
            working_dir = request.form['working_dir']
            parameters = request.form['parameters']

            # create the working directory for this job
            directory = create_directories(execution)

            copy_working_dir_contents(working_dir, directory)

            exec_file = "%s/%s" % (directory, executable.split('/')[-1])

            submit_file = create_submission_files(execution, directory, 
                                                  exec_file)

            modify_exec_file(execution, directory, exec_file, parameters)
            #Kill any logs that are currently running
            terminate_process("pmlogger")

            start_logging(execution)
            job_id = submit_job(submit_file)
            if job_id is None:
                print "condor_submit failed for job"
                return

            return job_id
    except Exception, e:
        print e
        raise e


@app.route('/execute_docker', methods=['POST'])
def handle_docker_request():
    try:

        if request.method == 'POST':
            executable = request.form['executable']
            execution = request.form['execution']
            working_dir = request.form['working_dir']
            parameters = request.form['parameters']

            # create the working directory for this job
            directory = create_directories(execution)

            copy_working_dir_contents(working_dir, directory)

            #Kill any logs that are currently running
            terminate_process("pmlogger")

            start_logging(execution)
            job_id = submit_docker_job(executable, execution)
            if job_id is None:
                print "docker submit failed for job"
                return

            return job_id
    except Exception, e:
        print e
        raise e

def start_logging(name):
    dest = "/home/ubuntu/logs/Job-%s" % name
    if not os.path.exists(dest):
        os.makedirs(dest)
    log = "%s/Job-%s" % (dest, name)
    try:
        cmd = ['pmlogger', '-c', '/etc/pcp/pmlogger/config.default', 
               '-t', '5', log]
        submit = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                                  stderr=subprocess.PIPE, 
                                  stdin=subprocess.PIPE) 
    except Exception, e:
        print "Failed to start logging"
        print e
        raise e


def terminate_process(name):
    for proc in psutil.process_iter():
        if proc.name() == name:
            try:
                proc.kill()
            except Exception, e:
                print e

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug = False)

