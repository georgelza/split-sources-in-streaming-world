FROM postgres:12

USER root

RUN mkdir -p /var/log/postgresql/
RUN chown -R postgres:postgres /var/log/postgresql
RUN chmod -R 777 /var/log/postgresql/

USER postgres

#copy some configuration files... this has been moved to the configs section of the docker compose
#COPY conf/postgresql.conf /etc/postgresql/postgresql.conf
#COPY conf/pg_hba.conf /etc/postgresql/pg_hba.conf

# Override default Postgres config file, this file is pushed in via a configs primitive setting in docker compose file.
# Additionally, we also override the default pg_hba.conf file using the same configs primitive setting in the docker compose file.
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]

