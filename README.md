# fake_api_ingestion

## Repository Structure
- `docker_files/`: Contains Docker configuration and setup files.
- `original_files/`: Original data sent to me used in the project (unedited).
- `tests/`: Unit and integration tests to validate the project functionality.
- `utils/`: Utility scripts or helper functions used throughout the project.
- `postgres_run.sh`: Shell script to run/configure PostgreSQL container
- `processing.py`: Main script or module responsible for data processing

## Getting Started

### Prerequisites
- Docker Desktop installed and running.
- Python 3 installed as the main version.
- Bash (or WSL on Windows) available on the machine.
- Ensure there is no existing Docker image named local-postgres
- This repo has been cloned locally

### Assumptions 
- Last day of the month exchange rate has been used when finding the monthly fee
- Fake api is already running at `http://localhost:5000`

### Usage
Setup environment
- Create a `venv` and install the packages listed in `requirements.txt`. (not the one on in original files)

Start postgres container
- Navigate to the repo within bash/wsl and run
```
bash ./postgres_run.sh start
```
- `start` can be replaced with `stop` and `status` to see if the container is running

Run processing script
- Make sure this terminal is connectd to the venv
- Start the python file, indicating the name of the config file (located in utils) to use
```
python ./processing.py config.ini
```
- This will produce `final_table.csv` in the same folder which contains the data.
- This can also be queried from the database using psql etc.