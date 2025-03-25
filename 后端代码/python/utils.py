# Description: Utility functions for the project
import base64
import json
import time


def pic2bs64(pic_path):
    """
    Convert a picture to base64 string
    :param pic_path: a path to the picture
    :type pic_path: str
    :return: a base64 string
    :rtype: str
    """
    with open(pic_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')


def bs642pic(bs64, pic_path):
    """
    Convert a base64 string to a picture
    :param bs64: a base64 string
    :type bs64: str
    :param pic_path: a path to save the picture
    :type pic_path: str
    :return: no return value
    :rtype: void
    """
    with open(pic_path, 'wb') as f:
        f.write(base64.b64decode(bs64))


def print_and_record(log):
    """
    Print log and record it to log.txt
    :param log: log message
    :type log: str
    :return: no return value
    :rtype: void
    """
    datestr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    head = log[: log.find('{')]
    content = log[log.find('{'):]

    try:
        dic = json.loads(content)
        # remove the data field
        if 'data' in dic:
            dic['data'] = '...'
        content = json.dumps(dic)
    except json.JSONDecodeError:
        pass

    print(head + content)
    with open('log.txt', 'a') as f:
        f.write(f"{datestr} : {log}\n")


if __name__ == '__main__':
    pass
