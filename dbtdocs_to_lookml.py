import lkml
import yaml ### install the pyyaml package

import os
import json

import time
import git  # Install `pip install GitPython`
from git import RemoteProgress

import csv
from lookerapi import LookerApi

# Hardcoded paths that should be parameteized later
PATH_TO_DBT_PROJECT = "PATH_TO_DBT_PROJECT"
GIT_URL = "https://github.com"
BRANCH =  "looker_dev_branch_name"
REPO_NAME = GIT_URL.split('/')[-1].split('.')[-2]
CLONE_LOOKML_PATH = "lookml_project"
PATH_TO_LOOKML_PROJECT = CLONE_LOOKML_PATH + "/" + REPO_NAME
PATH_TO_LOOKML_PROJECT_VIEWS = PATH_TO_LOOKML_PROJECT + "/views"

MANIFEST_FILE = "../target/manifest.json"
COMPILATION_MESSAGE = "You may need to run dbt compile first."


class CloneProgress(RemoteProgress):
    def update(self, op_code, cur_count, max_count=None, message=''):
        if message:
            print(message)


def get_lookml_files(GIT_URL, BRANCH, PATH_TO_LOOKML_PROJECT):
    """
    Download lookml files from github.
    """
    try:
        if PATH_TO_LOOKML_PROJECT_VIEWS:
            print('Pulling from %s' % GIT_URL)
        else:
            print('Cloning from %s' % GIT_URL)
        git.Repo.clone_from(url=GIT_URL, to_path=CLONE_LOOKML_PATH,
                            branch= BRANCH, progress=CloneProgress())
    except:
        pass

def push_lookml_files(GIT_URL, BRANCH, PATH_TO_LOOKML_PROJECT):
    """
    Push lookml files from github. (Not finished yet.)
    """
    try:
        print('Pushing to %s' % GIT_URL)
        git.Repo.add(url=GIT_URL, to_path=CLONE_LOOKML_PATH,
                            branch= BRANCH, progress=CloneProgress())

        git.Repo.commit(url=GIT_URL, to_path=CLONE_LOOKML_PATH,
                            branch= BRANCH, progress=CloneProgress())

        git.Repo.push(url=GIT_URL, to_path=CLONE_LOOKML_PATH,
                            branch= BRANCH, progress=CloneProgress())
    except:
        pass


def get_manifest(MANIFEST_FILE):
    """
    Parse the manifest file as this is faster than importing the manifest object
    from dbt.
    """
    manifest_path = os.path.join(MANIFEST_FILE)
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        return manifest
    except IOError:
        raise Exception(
            "Could not find {} file. {}".format(MANIFEST_FILE, COMPILATION_MESSAGE)
        )


def get_column_description(manifest, model_name, column_name):
    matching_models = []
    for fqn, node in manifest["nodes"].items():
        if node["name"] == model_name and node["resource_type"] == "model":
            matching_models.append(node)
    try:
        model = matching_models[0]
    except:
        return None

    try:
        column = model["columns"].get(column_name)
        col_description = column.get("description")
        return col_description
    except:
        return None


# load the manifest.json file -- just do this once
manifest = get_manifest(MANIFEST_FILE)

# Grab all lookml files
get_lookml_files(GIT_URL, BRANCH, PATH_TO_LOOKML_PROJECT)

# For each file in the lookml project
for file_name in os.listdir(PATH_TO_LOOKML_PROJECT_VIEWS):
    path_to_lookml_file = os.path.join(PATH_TO_LOOKML_PROJECT_VIEWS, file_name)

    # load the lookml
    print('Loading Looker View : ', file_name)
    with open(path_to_lookml_file, "r") as file:
        lookml = lkml.load(file)

    # for each view in the lookml
    for view in lookml["views"]:
        view_name = view["name"]

        # for each dimension in the view
        for dimension in view["dimensions"]:
            dimension_name = dimension["name"]
            dimension_description = get_column_description(
                manifest, view_name, dimension_name
            )
            if dimension_description:
                # update the description based on the project's description
                dimension["description"] = dimension_description
                print('Description found   : ', dimension_name + ':: ' + dimension_description)

    # dump the lmkl to a target directory
    os.makedirs(PATH_TO_LOOKML_PROJECT_VIEWS, exist_ok=True)
    target_lookml_file = os.path.join(PATH_TO_LOOKML_PROJECT_VIEWS, file_name)
    with open(target_lookml_file, "w+") as file:
        lkml.dump(lookml, file)
    print('Finished Looker View: ', file_name)
    print('\n')


# # Validate if new LookML files are valid
# #push_lookml_files(GIT_URL, BRANCH, PATH_TO_LOOKML_PROJECT) # TBD (didn't have time)
#
#
# ### Requires API v. 3.1 -- set this in config.yml
#
# ### ------- HERE ARE PARAMETERS TO CONFIGURE -------
# host = 'HOSTNAME'
#
# ### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------
# f = open('config.yml')
# params = yaml.load(f, Loader=yaml.FullLoader)
# f.close()
#
# my_host = params['hosts'][host]['host']
# my_project_id = params['hosts'][host]['project_id']
# my_secret = params['hosts'][host]['secret']
# my_token = params['hosts'][host]['token']
#
# looker = LookerApi(host=my_host,
#                  token=my_token,
#                  secret = my_secret)
#
# ### ------- GET AND PRINT THE LOOKML RESULTS -------
# data = looker.validate_project(my_project_id)
# print("LookML validation run results: ", data)
#
# ### ------- Done -------
# print('Done. {} errors found.'.format(length(data['errors'])))




# To-do:
# - multiple lkml files
# - parameterize the paths / add a cli
# - Consider what is a reasonable assumption for matching views to models? the view name?
# - error handling for unmatched view
