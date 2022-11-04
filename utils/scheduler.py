from typing import Optional

import asyncio
import pickle

from settings import QUEUE_FILE, SCHEDULER_INTERVAL, SCHEDULER_MAX_RATE, get_logger
from utils.job import Job


logger = get_logger()


class Scheduler:

    def __init__(self,
                 max_rate: int = SCHEDULER_MAX_RATE,
                 interval: int = SCHEDULER_INTERVAL,
                 concurrent_level: Optional[int] = None,
                 ):
        self.max_rate: int = max_rate  # число одновременно запущенных задач
        self.is_running: bool = False
        self._queue: asyncio.Queue = asyncio.Queue()  # очередь задач
        self.interval: int = interval  # пауза перед запуском следующего чанка задач

        # Если время выполнения задач больше интервала активизации планировщика (interval),
        # он накидывает дополнительные задачи сверху тех, которые еще не выполнились.
        # В таком случае количество параллельных запросов к сервису за interval будет больше rate_limit
        # concurrent_level ограничивает число одновременных запросов
        self.concurrent_level: Optional[int] = concurrent_level
        self._scheduler_task: Optional[asyncio.Task] = None
        self._sem: asyncio.Semaphore = asyncio.Semaphore(self.concurrent_level or self.max_rate)
        self._concurrent_workers: int = 0
        self._stop_event = asyncio.Event()
        self.dependence_dict: dict[int, asyncio.Event] = {}

    async def _scheduler(self):
        while self.is_running:
            # раз в interval ставит новые задачи и запускает _worker
            for _ in range(self.max_rate):
                async with self._sem:
                    task = await self._queue.get()
                    asyncio.create_task(self._worker(task))
            await asyncio.sleep(self.interval)

    def start(self):
        self.is_running = True
        # запускаем таску с самим планировщиком
        self._scheduler_task = asyncio.create_task(self._scheduler())

    async def recovery_queue(self):
        # читаем таски из файла в отдельном потоке
        tasks = await asyncio.to_thread(recovery_tasks, QUEUE_FILE)
        # наполняем очередь не выполненными ранее тасками
        [await self.put(task) for task in tasks]

    async def _get_not_completed_tasks(self):
        not_completed_tasks = []
        while not self._queue.empty():
            t = await self._queue.get()
            not_completed_tasks.append(t)

        return not_completed_tasks

    async def stop(self):
        self.is_running = False
        self._scheduler_task.cancel()

        not_completed_tasks = []
        if self._concurrent_workers != 0:
            # если при остановке еще выполняются таски, ждем их завершения
            await self._stop_event.wait()
            # собираем не выполненные таски для сохранения
            not_completed_tasks = await self._get_not_completed_tasks()
        # для асинхронного взаимодействия с файловой системой пишем в файл в отдельном потоке
        await asyncio.to_thread(save_tasks, QUEUE_FILE, not_completed_tasks)

    async def put(self, task: Job):
        await self._queue.put(task)

    async def join(self):
        # ждет, пока очередь не станет пустой
        await self._queue.join()

    async def _dependency_check(self, task: Job):
        if task.dependence:
            event = asyncio.Event()
            self.dependence_dict[task.dependence] = event
            await event.wait()
            self.dependence_dict.pop(task.dependence)

    async def _dependency_release(self, task: Job):
        if task.tid in self.dependence_dict:
            event = self.dependence_dict[task.tid]
            event.set()

    async def _worker(self, task: Job):
        # проверяем, есть ли у задачи зависимость
        await self._dependency_check(task)
        async with self._sem:
            # выполняет задачу и помечает ее выполненной
            self._concurrent_workers += 1
            await task.perform(self)
            self._queue.task_done()
            # сигнализируем зависящим задачам о выполнении
            await self._dependency_release(task)

        self._concurrent_workers -= 1
        # если планировщик остановлен и это последняя таска, шлем сигнал
        if not self.is_running and self._concurrent_workers == 0:
            self._stop_event.set()


def save_tasks(filename, tasks: asyncio.Queue):
    with open(filename, 'wb') as f:
        pickle.dump(tasks, f, protocol=pickle.HIGHEST_PROTOCOL)


def recovery_tasks(filename: str):
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        logger.info('Не найден .lock файл')
        return []
