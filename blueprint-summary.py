import requests
import asyncio
import aiohttp
import yaml
from yaml.loader import SafeLoader
import csv
import sys
import bwl_utils
import time
import logging
import argparse
from tqdm import tqdm

'''
This script creates an extract of all blueprints in the account visible to the 
service account you will have created in the bwl admin screens.

Each blueprint is listed in a csv file showing various meta-data such as age.
'''

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="config file name")
args = parser.parse_args()
config_filename = args.config

if config_filename is None:
    # Default config filename
    config_filename = "config.yaml"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
file_handler = logging.FileHandler('bwl-util.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

# Load the config
try:
    with open(config_filename, 'r') as config_file:
        config = yaml.load(config_file, Loader=SafeLoader)
except FileNotFoundError as e:
    msg = "Cannot find config file : config.yaml or the config file specified in arguments"
    print(msg)
    logger.error(msg)
    sys.exit("BWL API Util - aborting.")


# Get config for blueworks live URL, client_id and client_secret
ROOT_URL = config['root-url']
AUTH_URL = ROOT_URL + "/oauth/token"
CLIENT_REPORTING_ID = config['artefact-reporting-client-id']
CLIENT_REPORTING_SECRET = config['artefact-reporting-client-secret']
BLUEPRINT_LIB_URL = ROOT_URL + "/scr/api/LibraryArtifact?type=BLUEPRINT&returnFields=ID"

CLIENT_REPORTING_AUTH_DATA = {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_REPORTING_ID,
    'client_secret': CLIENT_REPORTING_SECRET
}

# Get the access token here
try:
    response = requests.post(AUTH_URL, data=CLIENT_REPORTING_AUTH_DATA)
    access_token = response.json()['access_token']
    if not access_token:
        raise ValueError('Access token could not be retrieved, please check your input')
except ValueError as e:
    logger.warning(e)
    print(e)
    exit()

CLIENT_REPORTING_ACCESS_TOKEN = access_token


async def get_blueprint_summaries(blueprint_list, bp_export, bp_errors):
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        pbar = tqdm(total=len(blueprint_list))
        for bp_id in blueprint_list:
            bp_id = bp_id.strip('/"')
            task = asyncio.create_task(get_blueprint_data(session, bp_id, bp_export, bp_errors, pbar))
            tasks.append(task)

        await asyncio.gather(*tasks)
        pbar.close()


async def get_blueprint_data(session, bp_id, bp_export, bp_errors, pbar):
    bp_url = ROOT_URL + "/bwl/blueprints/" + bp_id

    auth_value = f"Bearer {CLIENT_REPORTING_ACCESS_TOKEN}"
    head = {
        'Authorization': auth_value
    }

    async with session.get(bp_url, headers=head, ssl=False) as response:
        try:
            status = response.status
            if status == 200:
                bp_json = await response.json()
                bp_name = bwl_utils.get_name(bp_json)
                space_name = bwl_utils.get_space_name(bp_json)
                lmd = bwl_utils.get_last_modified_date(bp_json)
                age = bwl_utils.get_age(bp_json)

                bp_record = {'ID': bp_id, 'name': bp_name, 'space': space_name, 'last-modified': lmd, 'age': age}
                bp_export.append(bp_record)

                message = f"Finished processing blueprint ID : {bp_id}, Space : {space_name}, Name : {bp_name}"
                logger.debug(message)

            else:
                message = f"Error processing blueprint : {bp_id}, response code from BWL : {status}"
                logger.warning(message)
                bp_error = {'ID': bp_id}
                bp_errors.append(bp_error)

        except Exception as e:
            bp_error = {'ID': bp_id}
            bp_errors.append(bp_error)
            message = f"Unexpected error processing blueprint : {bp_id}"
            logger.error(message)
            logger.error(e)

        finally:
            pbar.update(1)


def get_blueprint_list():
    auth_value = f"Bearer {CLIENT_REPORTING_ACCESS_TOKEN}"
    head = {
        'Authorization': auth_value
    }

    blueprint_lib_response = requests.get(BLUEPRINT_LIB_URL, headers=head).text
    blueprint_list = blueprint_lib_response.split('\n')

    # remove the first & last elements
    # First is header, last is blank due to the final line break
    blueprint_list = blueprint_list[1:-1]

    return blueprint_list


def main():
    start_time = time.time()
    msg = 'Starting BWL Summary Extract'
    print(msg)
    logger.info(msg)

    blueprint_list = get_blueprint_list()
    msg = f"Found {len(blueprint_list)} blueprints"
    print(msg)
    logger.info(msg)

    # Create lists for the output
    bp_export = []
    bp_errors = []

    asyncio.run(get_blueprint_summaries(blueprint_list, bp_export, bp_errors))

    # Save the data, use a context manager to handle the closure
    with open('data_file.csv', 'w') as data_file:

        # Standard headers
        header = ['ID', 'Name', 'Space', 'LMD', 'Age in Days']
        csv_writer = csv.writer(data_file)
        row_count = 0
        for bp_record in bp_export:
            if row_count == 0:
                csv_writer.writerow(header)
                row_count += 1

            csv_writer.writerow(bp_record.values())

    print("--- %s seconds ---" % (time.time() - start_time))
    logger.info('Finished')


if __name__ == "__main__":
    main()