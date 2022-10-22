import os
import pytest
import shutil


@pytest.fixture
def create_tmp_dir():
    os.mkdir('tmp')
    open('tmp/file.txt', 'a').close()
    yield
    shutil.rmtree('tmp', ignore_errors=True)
