import re

def filter_list(list: list, filter: list) -> list:
    return [item for item in list if item not in filter]

def filter_list_by_regex(list: list, filter: str) -> list:
    return [item for item in list if re.search(filter, item) is None]

if __name__ == '__main__':
    list1 = ['Freddie', 'fucked', 'himself', 'for', '3', 'days', 'and', 'got', '100ml', 'milk']
    list2 = ['to', 'for', 'and']
    list1 = filter_list(list1, list2)
    result = filter_list_by_regex(list1, '[0-9]+')
    print(result)