import psycopg2
import requests
import sys
import logging
import time
import os
from configparser import ConfigParser
from urllib.parse import urljoin
import pandas as pd
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

def create_logger(script_name):
    '''Creates the logger object for the script which writes to the console and a file that has the date as an extension to it'''

    # get the current date (year,month,day)
    date_string = time.strftime("%Y-%m-%d", time.localtime())

    # Get log directory, by getting the folder above and then join it to logs
    script_path = os.path.abspath(script_name)
    script_dir = os.path.dirname(script_path)
    log_folder = os.path.abspath(os.path.join(script_dir, '..','logs'))

    # Create the log file name in the same directory as the script, with the filedate to differentiate
    filename = os.path.join(log_folder,f"truvi_logger_{date_string}.log")

    # create the log object and set its level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG) 

    # create 2 handlers
    # filehandler writes to the log file and will have all messages
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.DEBUG)
    # consoler handler prints to console but only shows info and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # create the general format for both loggers
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # Set the formatter for both
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def parse_config(logger,filename,required_section):
    """Check the config file is valid and contains the needed variables"""

    # Create parser and read config file
    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(required_section):
        msg = f"Required section '{required_section}' not found in the {filename} file"
        logger.critical(msg)
        raise ValueError(msg)

    # Convert all sections to a dict of dicts
    config_dict = {
        section: {
            # convert any values to integer where possible
            k: int(v) if v.isdigit() else v
            for k, v in parser.items(section)
        }
        for section in parser.sections()
    }

    return config_dict

def query_local_db(logger,query,engine):
    '''Execute a query against the local database from the config file'''

    try:
        # create connection
        with engine.begin() as conn:
            # get the raw psycopg2 connection
            raw_conn = conn.connection  
            # create cursor and execute query
            with raw_conn.cursor() as cur:
                cur.execute(query)

                # check if there is something to return and return it otherwise have the list that has nothing to return
                if cur.description is not None:
                    # If the cursor description is not None, it means there's a result set to fetch
                    result = cur.fetchall()
                else:
                    result = ["No data to return"]

                # begin automatically commits
                logger.debug(f"{query} executed")
                return result
    except (psycopg2.OperationalError,psycopg2.ProgrammingError,psycopg2.IntegrityError,psycopg2.errors.RaiseException) as e:
        # operational covers authentication and connection 
        # programming covers syntax errors in query as well as nonexistent tables and columns
        # integrity covers key and unique key violations
        # psycopg2.errors.RaiseException catches written exceptions within a function 
        # anything else will break the script as something is badly wrong

        # log the error and reraise across the stack
        logger.critical(f"{query} failed with {e}")
        raise

class Truvi():
    """Encapsulting all the logic into a class"""

    def __init__(self,logger,config_dict,engine,script_dir):

        # assign the logger, engine, and working directory
        self.logger=logger
        self.engine=engine
        self.output_folder = os.path.abspath(os.path.join(script_dir, '..'))

        # loop through the dict of dicts and assign them as attributes
        for section, params in config_dict.items():
            setattr(self, section, params)

    def get_api_data(self,api_parameters):
        """Connect to the api and return the results as a pandas dataframe"""

        # connect to the api
        response=requests.get(urljoin(self.api['base_url'],"api/bookings"),params=api_parameters)
        response.raise_for_status()

        # convert the response into json and get results from the dictionary
        data = response.json()
        results=data['results']

        # convert that into a dataframe
        df = pd.DataFrame(results)

        # then get the details from the response to deal with pagination
        page_info = {
            "page": int(data['page']),
            "per_page": int(data['per_page']),
            "total": int(data['total']),
        }

        # return these for the next functions
        return df,page_info
    
    def check_write_df(self,df):
        """Validate the dataframe has correct values as expected and then write to the database"""

        if df.empty:
            self.logger.info("Empty DataFrame")
            return
        
        # basic validation by checking the number of columns (would normally do more)
        cols= len(df.columns)
        if cols != 5:
            raise IndexError(f"df has {cols} columns, expected 5")

        # convert the columns into the correct datatype
        for key in self.table_values:
            if key in df.columns:
                df[key] = pd.to_datetime(df[key])
                self.logger.debug(f"Converted {key} to datetime")

        # write the dataframe to the database in append mode
        df.to_sql(schema="data",name="raw_data", con=self.engine, if_exists="append", index=False)
        self.logger.debug("Dataframe written to database")

    def post_processing(self):
        """Get the final table from the data"""

        # view and table exist within the database so get it out of there
        data=query_local_db(
            self.logger,
            "SELECT * FROM data.final_table;",
            self.engine
        )

        if len(data) == 0:
            raise ValueError("No data in view, please check")
        
        # get the column names
        raw_cols=query_local_db(
            self.logger,
            """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'data' AND table_name = 'final_table'
            """,
            self.engine
        )
        # get them out of the tuple
        columns = [row[0] for row in raw_cols]

        # create the final df and file path
        df = pd.DataFrame(data,columns=columns)
        output_path=os.path.join(self.output_folder,'final_table.csv')
        
        # save to csv
        df.to_csv(
            output_path,
            index=False
        )
        self.logger.info(f"Final table written to csv in {output_path}")

    def main(self):

        # get the intial parameters that will be replaced during the loop
        api_parameters=self.api_filters

        try:
            # make sure its empty incase runnng multiple times
            _=query_local_db(
                self.logger,
                "truncate table data.raw_data;",
                self.engine
            )

            # while loop to make sure all pages have been completed
            while True:
                # connect to the api and get the data as a dataframe
                df,page_info=self.get_api_data(api_parameters)
                self.logger.info(f"Fetched page {page_info['page']} with {len(df)} results")

                # write the dataframe to the db after cleaning
                self.check_write_df(df)

                # break out of the loop there should be no more results in the pages
                if page_info["page"] * page_info["per_page"] >= page_info["total"]:
                    self.logger.info("All pages fetched")
                    break

                # increase the page number to continue with the loop
                api_parameters["page"] += 1

            # post processing to create the final table
            self.post_processing()

        except Exception as e:
            self.logger.critical(f"Processing failed with {e}")

        finally:
            self.logger.info("Object function stopped")

def main():
    """Put everything into one main script"""

    # create the logger for the script in the same folder the script is running in
    logger=create_logger(__file__)

    # CLI arguments
    if len(sys.argv) != 2:
        logger.critical(f"Invalid number of CLI arguments: {sys.argv}")
        logger.critical("Usage: python script_name.py config_file_name")
        sys.exit(1)
    logger.debug(f"Starting {__file__}")

    # get the current path of the script needed for other functions
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # make sure the config file is valid
    config_dict = parse_config(
        logger,
        os.path.join(script_dir,sys.argv[1]),
        'Database'
    )

    # create the engine
    db_url = URL.create(**config_dict['Database'])
    engine=create_engine(db_url)

    # validate the password against the db
    _=query_local_db(
        logger,
        "SELECT 'janwashere';",
        engine
    )
    logger.info(f"Connection to database valid")

    # create an instance of the object and then run the function
    obj=Truvi(logger,config_dict,engine,script_dir)
    obj.main()

    logger.info("Script complete")
    # explicitly return 0
    return 0