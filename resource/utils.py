import os,sys,json
def openJson(filepath):
    """ open a json file and return a dict """
    if isinstance(filepath, str) and os.path.exists(filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
        return data