import json
import os


def has_cache(file_name):
    file_path = f'{file_name}.json'
    if os.path.exists(file_path):
        return True
    else:
        return False


def write_cache(file_name, obj):
    if has_cache(file_name):
        return

    file_path = f'{file_name}.json'
    json_str = json.dumps(obj)
    with open(file_path, 'w+') as json_file:
        json_file.write(json_str)


def override_cache(file_name, obj):
    file_path = f'{file_name}.json'
    json_str = json.dumps(obj)
    with open(file_path, 'w+') as json_file:
        json_file.write(json_str)


def read_cache(file_name):
    if has_cache(file_name):
        file_path = f'{file_name}.json'
        with open(file_path, 'r') as json_file:
            obj = json.load(json_file)
        return obj
    else:
        return None




