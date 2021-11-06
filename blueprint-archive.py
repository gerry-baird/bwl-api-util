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
logger.setLevel(logging.DEBUG)

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
CLIENT_AUTHORING_ID = config['artefact-authoring-client-id']
CLIENT_AUTHORING_SECRET = config['artefact-authoring-client-secret']
BLUEPRINT_LIB_URL = ROOT_URL + "/scr/api/LibraryArtifact?type=BLUEPRINT&returnFields=ID"
SOURCE_SPACE_ID = config['source-space-id']
BLUEPRINT_ARCHIVE_AGE_THRESHOLD = config['blueprint-archive-age-threshold']


# Retrieve the reporting access token
CLIENT_REPORTING_AUTH_DATA = {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_REPORTING_ID,
    'client_secret': CLIENT_REPORTING_SECRET
}

try:
    response = requests.post(AUTH_URL, data=CLIENT_REPORTING_AUTH_DATA)
    access_token = response.json()['access_token']
    if not access_token:
        raise ValueError('Reporting access token could not be retrieved, please check your input')
except ValueError as e:
    logger.warning(e)
    print(e)
    exit()

CLIENT_REPORTING_ACCESS_TOKEN = access_token

# Retrieve the authoring access token
CLIENT_AUTHORING_AUTH_DATA = {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_AUTHORING_ID,
    'client_secret': CLIENT_AUTHORING_SECRET
}

try:
    response = requests.post(AUTH_URL, data=CLIENT_AUTHORING_AUTH_DATA)
    auth_access_token = response.json()['access_token']
    if not access_token:
        raise ValueError('Authoring access token could not be retrieved, please check your input')
except ValueError as e:
    logger.warning(e)
    print(e)
    exit()

CLIENT_AUTHORING_ACCESS_TOKEN = auth_access_token


async def find_blueprints_for_archive(blueprint_list):
    connector = aiohttp.TCPConnector(limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        pbar = tqdm(total=len(blueprint_list))
        for bp_id in blueprint_list:
            bp_id = bp_id.strip('/"')
            task = asyncio.create_task(check_blueprint_for_archive(session, bp_id, pbar))
            tasks.append(task)

        await asyncio.gather(*tasks)
        pbar.close()


async def check_blueprint_for_archive(session, bp_id, pbar):
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
                age = bwl_utils.get_age(bp_json)
                archived_state = bp_json['archived-state']

                if age > BLUEPRINT_ARCHIVE_AGE_THRESHOLD and archived_state == "active":
                    message = f"Archiving Blueprint ID : {bp_id}, Space : {space_name}, Name : {bp_name}, State : {archived_state}."
                    logger.debug(message)
                    archive_blueprint(bp_id)
                else:
                    message = f"Skipping Blueprint ID : {bp_id}, Space : {space_name}, Name : {bp_name}, State : {archived_state}."
                    logger.debug(message)
            else:
                message = f"Error processing blueprint : {bp_id}, response code from BWL : {status}"
                logger.warning(message)

        except Exception as e:

            message = f"Unexpected error processing blueprint : {bp_id}"
            logger.error(message)
            logger.error(e)

        finally:
            pbar.update(1)

def archive_blueprint(bp_id):

    bp_archive_url = ROOT_URL + f"/bwl/artifacts/{bp_id}"
    auth_value = f"Bearer {CLIENT_AUTHORING_ACCESS_TOKEN}"
    headers = {
        'Authorization': auth_value
    }

    params = {
        'action': 'archive'
    }

    try:
        archive_response = requests.put(bp_archive_url, headers=headers, params=params)
        status = archive_response.status_code
        if status == 200:
            message = f"Archived Blueprint ID : {bp_id}"
            logger.debug(message)
        else:
            message = f"Error archiving Blueprint ID : {bp_id}, status code {status}"
            logger.debug(message)

    except Exception as e:
        logger.warning(e)
        print(e)



def get_blueprint_list():
    auth_value = f"Bearer {CLIENT_REPORTING_ACCESS_TOKEN}"
    head = {
        'Authorization': auth_value
    }
    blueprint_by_space_url = ROOT_URL + ('/scr/api/LibraryArtifact?type=BLUEPRINT&returnFields=ID&spaceId='
                                         + SOURCE_SPACE_ID)
    blueprint_lib_response = requests.get(blueprint_by_space_url, headers=head).text
    blueprint_list = blueprint_lib_response.split('\n')

    # remove the first & last elements
    # First is header, last is blank due to the final line break
    blueprint_list = blueprint_list[1:-1]

    return blueprint_list


def main():
    start_time = time.time()
    msg = 'Starting BWL Bulk Archive'
    print(msg)
    logger.info(msg)

    blueprint_list = get_blueprint_list()
    msg = f"Found {len(blueprint_list)} blueprints"
    print(msg)
    logger.info(msg)

    # create task list for blueprint archiv
    asyncio.run(find_blueprints_for_archive(blueprint_list))


    print("--- %s seconds ---" % (time.time() - start_time))
    logger.info('Finished')


if __name__ == "__main__":
    main()