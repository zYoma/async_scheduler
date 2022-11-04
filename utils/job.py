from typing import Callable, Optional

import asyncio
import datetime
from dataclasses import dataclass
from functools import wraps

from settings import START_AT_JOB_FORMAT, get_logger


logger = get_logger()


def trying(func):
    """ Декоратор для повторных попыток запуска задач в случае ошибок. Делает max_retries попыток.
        Постоянно удваивает паузу между попытками.
    """
    @wraps(func)
    async def wrap(self, *args, **kwargs):
        max_retries = self.max_retry
        count = 0
        while True:
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                count += 1
                if count > max_retries:
                    logger.exception(f'Задача {self.tid} отменена из-за ошибки {e}')
                    return wrap

                backoff = count * 2
                logger.error('Retrying task {} in {} seconds'.format(self.tid, backoff))
                await asyncio.sleep(backoff)
    return wrap


@dataclass
class Job:
    coroutine: Callable
    tid: int
    max_retry: int = 0
    timeout: Optional[int] = None
    start_at: Optional[str] = None
    dependence: Optional[int] = None
    kwargs: Optional[dict] = None

    @trying
    async def perform(self, scheduler):
        if not self.kwargs:
            self.kwargs = {}

        if self.start_at:
            await self._wait_until()

        logger.info(f'Запущена задача {self.tid}')
        try:
            await asyncio.wait_for(self.coroutine(**self.kwargs), self.timeout)
            logger.info(f'Задача {self.tid} выполнена')
        except asyncio.TimeoutError:
            logger.warning(f'Задача {self.tid} превысила время исполнения {self.timeout}с и была отменена')

    async def _wait_until(self):
        now = datetime.datetime.now()
        start_at = datetime.datetime.strptime(self.start_at, START_AT_JOB_FORMAT)
        # спим до заданного времени
        await asyncio.sleep((start_at - now).total_seconds())
