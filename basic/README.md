### basic airflow docker template
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow-in-docker

1 cluster with 8 containers: \
airflow-init
airflow-webserver
airflow-triggerer
airflow-scheduler
airflow-worker
flower --> queuing
postgres --> db
redis --> messaging among containers

### env
create .env file
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

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