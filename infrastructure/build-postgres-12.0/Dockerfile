FROM postgres:12

USER root

RUN mkdir -p /var/log/postgresql/
RUN chmod -R 777 /var/log/postgresql/

RUN mkdir -p /var/lib/postgresql/data
RUN chmod -R 777 /var/lib/postgresql/data


# Override default Postgres config file, this file is pushed in via a configs primitive setting in docker compose file.
# Additionally, we also override the default pg_hba.conf file using the same configs primitive setting in the docker compose file.
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]

