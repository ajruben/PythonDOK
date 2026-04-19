from pickle import TRUE
import geopandas as pd
import os
import shutil
import sys
import pathlib
import logging

from src import PDOKClient

#dir setup
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data/")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output/")
LOG_DIR = os.path.join(ROOT_DIR, "logs/")
DIRS = [ROOT_DIR, DATA_DIR, OUTPUT_DIR, LOG_DIR]

# orcherstrate
def main():
    setup_dirs()
    logger = setup_logger()
    logger.info("Setup PDOK Client")
    pdok_client = PDOKClient()
    logger.info("Get landing page NWB")
    landing_page = pdok_client.nwb_wegen.get_landing_page()
    logger.info("Get service description NWB")
    service_description = pdok_client.nwb_wegen.get_service_description()
    pass

def setup_dirs() -> None:
    for dir in DIRS:
    	os.makedirs(name = dir, exist_ok = True)

def setup_logger() -> logging.Logger:
	logger = logging.getLogger(__name__)
	logger.setLevel(level="DEBUG")
 
	format_file = logging.Formatter(
		fmt='[%(levelname)s]  [%(asctime)s] : %(message)s',
		datefmt = "%m-%d-%Y %I:%M:%S"
	)
	format_console = logging.Formatter(
		fmt='[%(levelname)s]  [%(asctime)s] : %(message)s',
		datefmt = "%I:%M:%S"
	)

	file_handler = logging.FileHandler(filename=os.path.join(LOG_DIR, "log.log"),
									mode="w",
									encoding="utf-8")
	file_handler.setFormatter(format_file)
 
	console_handler = logging.StreamHandler()
	console_handler.setFormatter(format_console)
	
	logger.addHandler(console_handler)
	logger.addHandler(file_handler)
	return logger

if __name__ == "__main__":
    main()
