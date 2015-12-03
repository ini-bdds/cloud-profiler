from flask import Flask, request, jsonify
import time
import datetime
import os
import ConfigParser
import subprocess

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
    print "Got a request"
    if request.method == 'POST':
        username = request.form['username']
        executable = request.form['executable']
        secret_key = ""
        access_key = ""
        key_pair = ""
        print "Request from %s" % username
        if 'access_key' in request.form:
            access_key = request.form['access_key']
            secret_key = request.form['secret_key']
            key_pair = request.form['key_pair']
        print "Access key %s" % access_key
        # Get the user id from the database
        res = {}
        try:

            db_manager = DBManager()
            print "Inserting to db"
            user_id = db_manager.get_user_id(username, access_key, secret_key,
                                             key_pair)
            print "Inserting new workload in db"
            # Now create a new workload
            workload_id = db_manager.create_new_workload(user_id)
            print "Creating working dir"
            workload_dir = create_workload_dir(workload_id)

            print "Updating db with working dir"
            # Update the database to reflect the working dir being there
            db_manager.update_workload_dir(workload_id, workload_dir,
                                           executable)

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
        
        # Create an entry in the db for each job
        db_jobs = {} 
        inst_params = {}
        db_manager = DBManager()

        db_manager.update_workload_dir(workload, working_dir, None)
        try:
            for inst in description['instance_types']:
                instance_name = inst['name']
                # instance_params = inst['override']
                job_id = db_manager.insert_job(workload)
                db_jobs.update({instance_name : job_id})
                inst_params.update({instance_name : instance_params})
        except Exception, e:
            print 'Error with job creation %s' % e
        # While we are at it, set the executable to execuable
        submit_line = "chmod 777 %s%s" % (working_dir, 
                                           description['executable'].split('/')[-1])
        submit = subprocess.Popen((['sudo','su','root','-c',submit_line]),
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
        s_out, s_err = submit.communicate()

        # Now start a thread for each of these jobs
        try:
            for inst, job_id in db_jobs.iteritems():
                instance_name = inst
                params = inst_params[instance_name]
                profiler_thread = ProfilerJobThread(config_file, job_id, 
                                                    inst, params, description['name'])
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
    print "I should be in here about to get some workload status..."
    db_manager = DBManager()
    workload_status = "Something went wrong."
    try:
        print "Getting workload status"
        workload_status = db_manager.get_workload_status(val)
        print workload_status
    except Exception, e:
        print e
        raise e
    print 'Somehow i am past this workload status thing now?'
    print 'returning %s' % workload_status
    return jsonify(workload_status)



#check job status - return the condor code for this job.
@app.route('/status/<string:val>')
def get_status(val):
    cmd = [ 'condor_history', '-format', '%s', 'JobStatus', '-constraint', 'ClusterId == %s' % val ]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    print output
    if float(output) >= 0:
        if output == '4':
            #The job is done, so stop the logger
            terminate_process("pmlogger")
        return output
    else:
        return "Job %s not in history" % val



@app.route('/logs/<string:val>')
def get_logs(val):
    dest = "/home/ubuntu/logs"
    if os.path.exists(dest):
        filename = "%s/Job-%s.tar" % (dest, val)     
        with tarfile.open(filename, "w:gz") as tar:
            tar.add("%s/Job-%s/" % (dest, val))
        return filename



@app.route('/sys_stats')
def get_sys_stats():
    data = {"message": "hello"}
    try:

        now = datetime.datetime.now().strftime('%s')
        cpu = psutil.cpu_times()
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_percent_per = psutil.cpu_percent(interval=1, percpu=True)
        network = psutil.net_io_counters()
        disk = psutil.disk_io_counters()
        disk_usage = psutil.disk_usage('/')
        mem = psutil.virtual_memory()
        # virtmem = psutil.virtmem_usage()

        print psutil.__version__
        print psutil.cpu_times()
        print psutil.net_io_counters()
        print psutil.disk_io_counters()
        print psutil.disk_usage('/')
        print psutil.virtual_memory()
        

        cpu_dict = {"system" : cpu.system, "user" : cpu.user, "idle" : cpu.idle, "iowait" : cpu.iowait, "percent" : cpu_percent, "percent_per_core" : cpu_percent_per}
        network_dict = {"bytes_sent" : network.bytes_sent, "bytes_recv" : network.bytes_recv, "packets_sent" : network.packets_sent, "packets_recv" : network.packets_recv}
        disk_dict = {"free" : disk_usage.free, "total" : disk_usage.total, "used" : disk_usage.used, "percent" : disk_usage.percent, "read_count" : disk.read_count, "write_count" : disk.write_count, "read_bytes" : disk.read_bytes, "write_bytes" : disk.write_bytes, "read_time" : disk.read_time, "write_time" : disk.write_time}
        mem_dict = {"total" : mem.total, "used" : mem.used, "percent" : mem.percent, "free" : mem.free, "available" : mem.available}
        

        data = {"date" : now, "cpu" : cpu_dict, "disk" : disk_dict, "network" : network_dict, "memory" : mem_dict}

        print data
        return jsonify(data)
    except Exception, e:
        print e


#get the monitored stats on the job.
@app.route('/stats/<string:val>')
def get_stats(val):
    cmd = [ 'condor_history', '-format', 'id:%s,', 'ClusterId', '-format', 'status:%s,', 'JobStatus','-format', 'time:%s', 'RemoteWallClockTime', '-constraint', 'ClusterId == %s' % val ]

    output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]
    print output
    if val in output:
        return output
    else:
        return "Job %s not in history" % val

def create_directories(execution):
    directory = "/home/ubuntu/job%s" % execution
    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory

def create_shell_script(execution, directory, executable):
    submit_line = "chmod 777 %s" % (directory)
    submit = subprocess.Popen( ( ['sudo','su','root','-c',submit_line ] ), stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    s_out, s_err = submit.communicate()

    print 'set dir permissions.'

    submit_line = "cp %s %s/" % (executable, directory)
    mylist = ['sudo','su','galaxy','-c',submit_line ]
    print " ".join(mylist)
    submit = subprocess.Popen( ( ['sudo','su','galaxy','-c',submit_line ] ), stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    s_out, s_err = submit.communicate()

    print executable
    print directory
    #now try to read the file - need to split to get the file name and use that with the dir
    exec_loc = "%s/%s" % (directory, executable.split("/")[-1])
    print exec_loc
    exec_script = ''
    with open(exec_loc, "r") as myfile:
        exec_script = myfile.readlines()
    print exec_script
    new_script = ''
    for line in exec_script:
        print line
        command_line = ''
        cmds = line.split(' ')
        for command in cmds:
            print command
            modified_line = command
            if "job_working_directory" in command:
                print 'replacing working dir'
                #woah, ok this makes no sense. there must be a better way to replace this...
                dir_str = command.split('job_working_directory')
                print dir_str
                modified_line = "%s%s" % (directory, dir_str[-1])
                print modified_line
            # now piece is all back together
            command_line = "%s%s " % (command_line, modified_line)
        if command_line == '':
            command_line = line
        new_script = "%s%s\n" % (new_script, command_line)

    print 'i got here?'
    print new_script
    exec_file = "%s/test%s.sh" % (directory, execution)
    print exec_file


    with open(exec_file, "w") as text_file:
        text_file.write(new_script)

    print "setting permissions on the new script too."

    submit_line = "chmod 777 %s" % (exec_file)
    submit = subprocess.Popen( ( ['sudo','su','root','-c',submit_line ] ), stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    s_out, s_err = submit.communicate()


    return exec_file

def create_submission_files(execution, directory, executable):


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

def submit_job(submit_file):
    job_id = None
    try:
        submit_line = "condor_submit %s" % submit_file
        submit = subprocess.Popen( ( ['sudo','su','ubuntu','-c',submit_line ] ), stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
        s_out, s_err = submit.communicate()
        if submit.returncode == 0:
            match = re.search( 'submitted to cluster (\\d+).', s_out )
            if match is None:
                s_out = 'Failed to find job id from condor_submit'
            else:
                job_id = match.group( 1 )
        print s_out
        print s_err
        print submit.returncode
        return job_id
    except Exception, e:
        print e
        raise e

#add a job to the condor queue
@app.route('/execute', methods=['POST'])
def handle_request():
    if request.method == 'POST':
        execution = request.form['execution']
        executable = request.form['executable']
        parameters = request.form['parameters']

        print "making directory"
        #create the working directory for this job
        directory = create_directories(execution)

        print "Modifying executable to use working dir"
        exec_file = create_shell_script(execution, directory, executable)
       
        print "Creating submit files"
        submit_file = create_submission_files(execution, directory, exec_file)

        #Kill any logs that are currently running
        terminate_process("pmlogger")

        print "Starting logger"
        start_logging(execution)
        print "Submitting job"
        job_id = submit_job(submit_file)
        print "Submitted job now"
        if job_id is None:
            log.debug( "condor_submit failed for job %s: %s" % (job_wrapper.get_id_tag(), s_out) )
            self.cleanup( ( submit_file, executable ) )
            job_wrapper.fail( "condor_submit failed", exception=True )
            return


        return job_id

def start_logging(name):
    dest = "/home/ubuntu/logs/Job-%s" % name
    if not os.path.exists(dest):
        os.makedirs(dest)
    log = "%s/Job-%s" % (dest, name)
    print "Creating log: %s" % log
    try:
        cmd = ['pmlogger', '-c', '/etc/pcp/pmlogger/config.default', '-t', '5', log]
        submit = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE) 
    except Exception, e:
        print "Failed to start logging"
        print e
        raise e

    print "Started the logger?"


def terminate_process(name):
    for proc in psutil.process_iter():
        if proc.name() == name:
            try:
                proc.kill()
            except Exception, e:
                print e

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug = False)

