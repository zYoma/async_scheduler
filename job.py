import shutil
from urllib.request import urlopen
from pathlib import Path
import ssl
import json
from logger import logger


def coroutine(f):
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap


def get_and_write_data(condition, url):
    context = ssl._create_unverified_context()
    file = 'punchline.txt'
    with condition:
        with urlopen(url, context=context) as req:
            resp = req.read().decode("utf-8")
            resp = json.loads(resp)
        if req.status != 200:
            raise Exception(
                "Error during execute request. {}: {}".format(
                    resp.status, resp.reason
                )
            )
        data = resp
        if isinstance(data, dict):
            path = Path(file)
            setup = data['setup']
            punchline = data['punchline']
            print(
                f'Setup: {setup} \n'
                f'Punchline: {punchline}'
            )
            with open(path, mode='a') as config:
                config.write(str(data))
        else:
            logger.error(type(data))
            logger.error(ValueError)
            raise ValueError


def copy_file(condition, x=None):
    file = 'punchline.txt'
    to_path = './jokes/'
    with condition:
        condition.wait(timeout=1)
        try:
            shutil.copy(file, to_path)
        except FileNotFoundError as ex:
            logger.error('Файл не найден %s', ex)


def delete_file(condition, x=None):
    file = 'punchline.txt'
    obj = Path(file)
    with condition:
        condition.wait(timeout=1)
        try:
            obj.unlink()
            logger.info('Удалил файл')
        except FileNotFoundError as ex:
            logger.error(ex)


class Job:
    def __init__(
            self,
            func=None,
            name=None,
            args=None,
    ):
        self.args = args
        self.name = name
        self.func = func

    def run(self, *args):
        tar = self.func(*args)
        logger.info('тип объекта в Job.run %s', type(tar))
        logger.debug('запуск объекта %s', tar)
        return tar