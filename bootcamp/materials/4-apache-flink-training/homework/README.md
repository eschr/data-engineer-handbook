## Flink session events execution:
1. ensure the docker container is running per instructions here https://github.com/eschr/data-engineer-handbook/blob/main/bootcamp/materials/4-apache-flink-training/README.md
2. exectue command `docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src -d` from command line.
3. This job uses 'earilest-offset' for the kafka source table.

## Postgres sink analysis:
1. ensure the docker container is running as described from week 1.
2. Execute the commands from the `flink_session_events.sql` file.
3. Expected results: first query will provide the average events per session for the tech creator host.
The second query compares the average events per session for the hosts 'zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io'

While I executed the job I got the following results for the second query:

lulu.techcreator.io, 1.7045454545454545
zachwilson.techcreator.io, 1.5833333333333333

