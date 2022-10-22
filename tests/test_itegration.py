import asyncio
import httpx
import os
import pytest
import shutil
from pathlib import Path

from main import main
from utils.job import Job
from utils.scheduler import Scheduler


async def copy_file(filename, to_path):
    shutil.copy(filename, to_path)


async def delete_file(filename):
    obj = Path(filename)
    obj.unlink()


async def create_dir(dirname):
    os.mkdir(dirname)


async def write_string_to_file(filename, string, with_n=False):
    sep = '\n' if with_n else ''
    with open(filename, 'a') as f:
        f.write(f'{string}{sep}')


async def get_request(url):
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        await write_string_to_file('tmp/file.txt', r.json().get('setup'), with_n=True)


@pytest.mark.asyncio
async def test_file_system_job(create_tmp_dir):
    # тестируем создание, копирование, удаление файлов
    jobs = [
        Job(tid=1, coroutine=copy_file, kwargs={'filename': 'tmp/file.txt', 'to_path': 'tmp/newfile.txt'}),
        Job(tid=2, coroutine=delete_file, kwargs={'filename': 'tmp/file.txt'}),
        Job(tid=3, coroutine=create_dir, kwargs={'dirname': 'tmp/new_dir'}),
    ]
    s = Scheduler(1)
    await main(s, jobs)
    await asyncio.sleep(5)

    assert os.listdir('tmp') == ['newfile.txt', 'new_dir']


@pytest.mark.asyncio
async def test_file_job(create_tmp_dir):
    # Тестируем запись в файл, причем у задач есть зависимости, обеспечивающие последовательность записи
    jobs = [
        Job(tid=1, coroutine=write_string_to_file,
            kwargs={'filename': 'tmp/file.txt', 'string': 'Bob'}, dependence=2),
        Job(tid=2, coroutine=write_string_to_file,
            kwargs={'filename': 'tmp/file.txt', 'string': ' name is '}, dependence=3),
        Job(tid=3, coroutine=write_string_to_file,
            kwargs={'filename': 'tmp/file.txt', 'string': 'My'}),
    ]
    s = Scheduler(3, concurrent_level=1)
    await main(s, jobs)
    await asyncio.sleep(5)

    with open('tmp/file.txt', 'r') as f:
        result = f.read()
    assert result == 'My name is Bob'


@pytest.mark.asyncio
async def test_requests(create_tmp_dir):
    # тестируем работу с сетевыми запросами
    url = 'https://official-joke-api.appspot.com/random_joke'
    jobs = [
        Job(tid=1, coroutine=get_request,
            kwargs={'url': url}),
        Job(tid=2, coroutine=get_request,
            kwargs={'url': url}),
        Job(tid=3, coroutine=get_request,
            kwargs={'url': url}),
    ]
    s = Scheduler(3)
    await main(s, jobs)
    await asyncio.sleep(2)

    with open('tmp/file.txt', 'r') as f:
        result = f.readlines()

    assert len(result) == 3
    assert all(result)
    assert all([isinstance(i, str) for i in result])
