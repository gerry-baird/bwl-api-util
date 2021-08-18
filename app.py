import requests
import aiohttp
import asyncio
import csv
import bwl_utils
import time
from keys import CLIENT_SECRET
from keys import CLIENT_ID

start_time = time.time()

AUTH_URL = "https://us001.blueworkslive.com/oauth/token"

AUTH_DATA = {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET
}

response = requests.post(AUTH_URL, data=AUTH_DATA)
access_token = response.json()['access_token']

print(f"Access Token : {access_token}")


BLUEPRINT_LIB_URL = "https://us001.blueworkslive.com/scr/api/LibraryArtifact?type=BLUEPRINT&returnFields=ID"
head = {
    'Authorization': 'Bearer {}'.format(access_token),
    # 'X-On-Behalf-Of' : 'mark_ketteman@uk.ibm.com'
}
blueprint_lib_response = requests.get(BLUEPRINT_LIB_URL, headers=head).text
blueprint_list = blueprint_lib_response.split('\n')

#remove the first element
blueprint_list = blueprint_list[1:]
print(f"Found {len(blueprint_list)} blueprints")

#Create lists for the output
bp_export = []
bp_errors = []

async def main():
    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for bp_id in blueprint_list:
            bp_id = bp_id.strip('/"')
            task = asyncio.ensure_future(get_blueprint_data(session, bp_id))
            tasks.append(task)

        await asyncio.gather(*tasks)

async def get_blueprint_data(session, bp_id):
    url = "https://us001.blueworkslive.com/bwl/blueprints/" + bp_id

    async with session.get(url, headers=head) as response:
        try:
            bp_json = await response.json()
            bp_name = bwl_utils.get_name(bp_json)
            space_name = bwl_utils.get_space_name(bp_json)
            lmd = bwl_utils.get_last_modified_date(bp_json)

            bp_record = {'ID': bp_id, 'name': bp_name, 'space': space_name, 'last-modified': lmd}
            bp_export.append(bp_record)
        except:
            bp_error = {'ID': bp_id}
            bp_errors.append(bp_error)

asyncio.run(main())

# Save the data
data_file = open('data_file.csv', 'w')
header = ['ID', 'Name', 'Space', 'LMD']
csv_writer = csv.writer(data_file)
row_count = 0
for bp_record in bp_export:
    if row_count == 0:
        csv_writer.writerow(header)
        row_count += 1

    csv_writer.writerow(bp_record.values())

data_file.close()

#Save any errors
error_file = open('error_file.csv', 'w')
header = ['ID']
csv_writer = csv.writer(error_file)
row_count = 0
for bp_record in bp_errors:
    if row_count == 0:
        csv_writer.writerow(header)
        row_count += 1

    csv_writer.writerow(bp_record.values())

error_file.close()


print("--- %s seconds ---" % (time.time() - start_time))