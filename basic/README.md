### basic airflow docker template
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow-in-docker

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
