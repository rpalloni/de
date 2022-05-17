### data
https://grouplens.org/datasets/movielens/

### makefile
create dist folder with the content to deliver to the cluster
$ make build


### spark submit
submit jobs to spark with additional commands
$ spark-submit \
  --py-files jobs.zip, shared.zip \ # add jobs with zip
  --files config.json \ # send config file to cluster
  main.py --job job1


spark-submit --files config.json main.py --job job1
spark-submit --files config.json main.py --job job2
