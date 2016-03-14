# cloud-profiler

Introduction

This project presents an autonomic profiler for tools on the cloud. The profiler was constructed to allow a user to easily test a tool over a wide set of instance types. The profiler has been used to construct profiles of various tools in order to determine the required resources and guide the selection of instance types when scheduling workloads on the cloud.

Installation

The profiler is comprised of four distinct components: the profiling service, the worker services, a database, and a client to initiate the profile requests. The deploy script has been designed to automate the majority of the installation of the profiling service. The script configures an instance and downloads the profiling service. It also connects to a designated database to ensure the required tables are created. 

To use the deployment script you must configure the ini file to specify the instance type and database location that should be used.

Once the profiling instance is created you are required to configure the profiling ini file before executing the profiling service. This is because the profiling service needs to know where the database is and your aws credentials to acquire instances.

Output

Provided everything goes well the resulting logs of the profiling service will be processed and converted to a csv document. The csv are currently uploaded to an AWS S3 bucket. A summary of the profiler’s collected information is also generated and returned to the client. This summary should look something like the following:

  "profiles": [
    {
      "exec_time": 10, 
      "exit_status": "Completed", 
      "instance": "r3.xlarge", 
      "results": {
        "cpu": {
          "Avg_Idle": 3992.842105263158, 
          "Avg_Sys": 2.0, 
          "Avg_User": 3.5789473684210527
        }, 
        "disk": {
          "Read_Bytes": 0.0, 
          "Write_Bytes": 390.0
        }, 
        "memory": {
          "Avg_Free": 29608525.47368421, 
          "Phys_Mem": 31416208.0
        }, 
        "network": {
          "Total_In": 76711.0, 
          "Total_Out": 19.0
        }
      }, 
      "status": "Complete"
    }
  ], 
  "workload": "339"
}

Contact

Please feel free to contact us at ryan@ecs.vuw.ac.nz and madduri@uchicag.edu. This work is supported by NIH Grants 5U54GM114838-02 and 1U54EB020406-01 
