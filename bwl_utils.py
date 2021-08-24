from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s%(name)s:%(message)s')
file_handler = logging.FileHandler('bwl-util.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



def get_name(blueprint):
    bp_name = blueprint['name']
    return bp_name

def get_space_name(blueprint):
    space_name = blueprint['space-names'][0]
    return space_name

def get_last_modified_date(blueprint):
    lmd = blueprint['last-modified-date']
    return lmd


def get_age(blueprint) -> int:
    # get the age in days from last edited or last published
    # by default use the las modified date as all blueprints have this
    date_to_use = 'last-modified-date'

    published_status = blueprint['published-state']
    if published_status == 'published':
        date_to_use = 'published-date'

    blueprint_date_str = blueprint[date_to_use]
    blueprint_date_str = blueprint_date_str[:10]

    blueprint_date = datetime.strptime(blueprint_date_str, '%Y-%m-%d')

    today = datetime.now()
    delta = today - blueprint_date
    blueprint_age = abs(delta.days)

    return blueprint_age

def get_days_since_published(blueprint):
    pub_state = blueprint['published-state']
    current_date = datetime.now(timezone.utc)

    if pub_state == "published":
        pub_date = datetime.strptime(blueprint['published-date'], "%Y-%m-%dT%H:%M:%S.%f%z")
#        current_date = (current_date, "%Y-%m-%dT%H:%M:%S.%f%z")
        dsp = abs((current_date - pub_date).days)
    else:
        dsp = 'N/A'
    return dsp

def get_published_state(blueprint):
    pub_state = blueprint['published-state']
    return pub_state

def get_published_date(blueprint):
    pub_date = 'N/A'
    pub_state = blueprint['published-state']
    if pub_state == "published":
        pub_date = blueprint['published-date']
    return pub_date

