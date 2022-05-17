### basic airflow docker template
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow-in-docker

1 cluster with 8 containers:
* airflow-init --> setup check and containers lunch
* airflow-webserver --> app
* airflow-triggerer -->
* airflow-scheduler --> assign tasks to workers
* airflow-worker --> run dags tasks
* flower --> queuing tasks
* postgres --> airflow data
* redis --> messaging among containers

### env
create .env file \
~~~
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
~~~

### config
WebApp user: default [Airflow, airflow] => in airflow-init set _AIRFLOW_WWW_USER_USERNAME _AIRFLOW_WWW_USER_PASSWORD \
FileSensor: create a new connection to read the files in the machine (web interface: Admin > Connections) \
[https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html]

### browser
airflow
http://localhost:8080/

celery
http://localhost:5555/

### inspect airflow-webserver
docker-compose exec airflow-webserver bash

### add/remove libs
poetry new <projectname>
poetry add <libname>
poetry remove <libname>

### airflow DAGs and tasks
DAG (Directed Acyclic Graph): scheduling layer of tasks pipeline with a unique direction
Tasks are atomic and indipendent from other tasks in the pipeline. \
For each task, save result in a file/db for subsequent processing \
use sensor to check file presence

xcom: save/retrieve task result (1GB) in a variable to be used by another task
* xcom_push
* xcom_pull

#### schedule_interval

| Frequency                                                         | @             |
| ----------------------------------------------------------------- | ------------- |
| Don't schedule; use exclusively "externally triggered" DAGs.      | None          |
| Schedule once and only once                                       | @once         |
| Run once an hour at the beginning of the hour                     | @hourly       |
| Run once a day at midnight | are neat                             | @daily        |
| Run once a week at midnight on Sunday morning                     | @weekly       |
| Run once a month at midnight on the first day of the month        | @monthly      |
| Run once a year at midnight of January 1                          | @yearly       |
