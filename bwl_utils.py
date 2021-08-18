
def get_name(blueprint):
    bp_name = blueprint['name']
    return bp_name

def get_space_name(blueprint):
    space_name = blueprint['space-names'][0]
    return space_name

def get_last_modified_date(blueprint):
    lmd = blueprint['last-modified-date']
    return lmd

