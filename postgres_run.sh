#!/bin/bash

# This a script to make runnning and starting the local postgres version easier

# hard coding values into this script for ease
pguser=salcedo
pgpass=a_password
pgdb=truvi_db
container_name=local-postgres

# get the current path of the script
dir_path=$(dirname "$(realpath "$0")")

# create the absolue path to the yml file
compose_file="${dir_path}/docker_files/docker-compose.yml"

# exit function if input is wrong or missing
usage() {
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
}

currencies() {
    # get the currency table onto the database

    # first copy the csv into the container
    docker cp "${dir_path}/docker_files/currency_rates.csv" ${container_name}:/tmp/currency_rates.csv

    # truncate and copy into the database
    docker exec -it $container_name psql -U $pguser -d $pgdb -c "
    TRUNCATE TABLE data.currency_conversions;
    COPY data.currency_conversions
    FROM '/tmp/currency_rates.csv' 
    WITH (FORMAT CSV, HEADER true);"
}

run_diff () {
    case "$1" in
        start)
            echo "Starting PostgreSQL..."
            docker compose -f "$compose_file" up -d
            # need to wait a bit as the container refuses connections at the start
            # can probably improve this by making a function that checks the health
            sleep 10
            currencies
            echo "Currencies added to database"
            ;;
        stop)
            echo "Stopping PostgreSQL..."
            docker compose -f "$compose_file" down
            ;;
        restart)
            echo "Restarting PostgreSQL..."
            docker compose -f "$compose_file" down
            docker compose -f "$compose_file" up -d
            ;;
        status)
            docker compose -f "$compose_file" ps
            ;;
        *)
            # run the function with no input to make it assume its an error
            usage
            ;;
    esac
}

main() {
    # run this if the input to the script is wrong
    if [ $# -ne 1 ]; then
        usage
    fi
    run_diff "$1"
    exit 0
}
main "$@"