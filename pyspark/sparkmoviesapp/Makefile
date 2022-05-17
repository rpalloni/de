all: test build

test:
	pytest tests

build:
	rm -rf ./dist && mkdir ./dist ./dist/data
	cp ./main.py ./dist
	cp ./config.json ./dist
	cp ./data/movies.csv ./dist/data
	zip -r dist/jobs.zip jobs
	zip -r dist/shared.zip shared
	# docker run ...
	# pip install -r requirements.txt -t ./dist/libs
	# cd ./dist/libs && zip -r -D ../libs.zip .
