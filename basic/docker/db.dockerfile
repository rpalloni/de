FROM postgres:14.2

RUN mkdir -p /project/backup/
RUN mkdir -p /project/query/

# launch postgres
CMD ["postgres"]
