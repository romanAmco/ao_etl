import subprocess
from decouple import config

def mount_sql_container(DOCKER_NAME_SQL,DOCKER_DATABASE,DOCKER_PASS,DOCKER_DB_PORT):
    remove_containers='docker rm -f $(docker ps -aq)'
    process = subprocess.Popen(remove_containers.split(), stdout=subprocess.PIPE)
    _, error = process.communicate()
    if error is not None:
        mount_containers=f"docker run -p {DOCKER_DB_PORT}:3306 --name {DOCKER_NAME_SQL}  -e MYSQL_ROOT_PASSWORD={DOCKER_PASS}  -e MYSQL_DATABASE={DOCKER_DATABASE}  -d mysql:5.7"
        process = subprocess.Popen(mount_containers.split(), stdout=subprocess.PIPE)
        _, error = process.communicate()
        if error is None:
            print('Error mounting container')
        else:
            print('Container mounted succesfully')
    else:
        print('Error deleting containers') 


def dump_container_sql(FILEPATHSQL,DOCKER_NAME_SQL,DOCKER_PASS,DOCKER_DATABASE):

    mount_dump=f"cat {FILEPATHSQL} | docker exec -i {DOCKER_NAME_SQL} /usr/bin/mysql -u root --password={DOCKER_PASS} {DOCKER_DATABASE}"
    process = subprocess.Popen(mount_dump.split(), stdout=subprocess.PIPE)
    _, error = process.communicate()
    if error is not None:
        print('Error mounting container')
    else:
        print('Container mounted succesfully')


if __name__ == "__main__":
    DOCKER_NAME_SQL=config('DOCKER_NAME_SQL')
    DOCKER_DATABASE=config('DOCKER_DATABASE')
    DOCKER_PASS=config('DOCKER_PASS')
    DOCKER_DB_PORT=config('DOCKER_DB_PORT')
    FILEPATHSQL=f"{config('PATH_DUMP')}/{config('FILE_SQL')}"
    try:
        mount_sql_container(DOCKER_NAME_SQL,DOCKER_DATABASE,DOCKER_PASS,DOCKER_DB_PORT)
        dump_container_sql(FILEPATHSQL,DOCKER_NAME_SQL,DOCKER_PASS,DOCKER_DATABASE)
    except:
        print('Error mounting docker container')



