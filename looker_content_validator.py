import csv
import yaml ### install the pyyaml package
from lookerapi import LookerApi
from pprint import pprint


### Requires API v. 3.1 -- set this in config.yml

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

host = 'HOSTNAME'
output_csv_name = 'output.csv'

### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------

f = open('config.yml')
params = yaml.load(f, Loader=yaml.FullLoader)
f.close()

my_host = params['hosts'][host]['host']
my_secret = params['hosts'][host]['secret']
my_token = params['hosts'][host]['token']

looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)


### ------- GET AND PRINT THE LOOK -------

space_data = looker.get_all_spaces(fields='id, parent_id, name')

data = looker.get_content_validation()


host_url = my_host[:my_host.index(":19999")]
broken_content = data['content_with_errors']
# pprint(broken_content)


output = []
for item in broken_content:
    if item['dashboard'] is None:
        type = 'look'
    else:
        type = 'dashboard'

    id = item[type]['id']
    name = item[type]['title']
    url =  '{}/{}s/{}'.format(host_url,
                             type,
                             id
                             )
    errors = item['errors']
    space_id =  item[type]['space']['id']
    space_name = item[type]['space']['name']
    space_url = '{}/spaces/{}'.format(host_url,
                                      space_id
                                      )

    if type == 'look':
        element = None
    else:
        dashboard_element = item['dashboard_element']
        element = dashboard_element['title'] if dashboard_element is not None else None
    # find parent_space

    space = next(item for item in space_data if item['id'] == space_id)
    parent_space_id = space['parent_id']
    if parent_space_id is None:
        parent_space_name = None
        parent_space_url = None
        pass
    else:
        parent_space_url = '{}/spaces/{}'.format(host_url,
                                                 parent_space_id
                                                 )
        parent_space = next((item for item in space_data if item['id'] == parent_space_id), None)
        if parent_space:
            parent_space_name = parent_space['name']
        else:
            parent_space_name = None
    data = {
            'type' : type,
            'name' : name,
            'dashboard_element': element,
            'url' : url,
            'space_name' : space_name,
            'parent_space_name': parent_space_name,
            'space_url' : space_url,
            'parent_space_url': parent_space_url,
            'errors': str(errors)
           }
    output.append(data)

try:
    with open(output_csv_name, 'w') as csvfile:
        writer = csv.DictWriter(csvfile,
                               fieldnames=list(output[0].keys()))
        writer.writeheader()
        for data in output:
            writer.writerow(data)
    print('Error information outputed to {}'.format(output_csv_name))
except IOError:
    print("I/O error")

### ------- Done -------

print("Done")
