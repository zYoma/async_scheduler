# Привет! В общем что смог, то сделал. Остальное даже загуглить не могу.
# По факту просто трачу время, сидя и пялясь в экран. На эту реализацию неделя потрачена))
# Я просто не понимаю даже что гуглить, что бы сделать всё задание.
# Не знаю, может после ревью я с мёртвой точки сдвинусь, поэтому отправляю пока так...

from job import Job, get_and_write_data, delete_file, copy_file
from logger import logger
import multiprocessing


def coroutine(f):
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap


class Scheduler(object):
    def __init__(
            self,
            max_working_time=1,
            tries=0,
            dependencies=(),
            start_at=None,
    ):
        super().__init__()
        self.task_list: list[Job] = []
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies if dependencies is not None else None

    @coroutine
    def schedule(self):
        processes = []
        while True:
            task_list = (yield)
            print(task_list)
            for task in task_list:
                logger.info(f'Планировщик: запускаю задачу - {task.name}')
                p = multiprocessing.Process(target=task.run, args=(condition, url),)
                p.start()
                processes.append(p)
            for process in processes:
                logger.info(process)
                process.join()
                logger.info(f' process {process} stopped!')

    def run(self, jobs: tuple):
        gen = self.schedule()
        gen.send(jobs)


if __name__ == '__main__':
    condition = multiprocessing.Condition()
    url = 'https://official-joke-api.appspot.com/random_joke'
    job1 = Job(
        func=get_and_write_data,
        name='Запрос в сеть',
        args=(condition, url),
    )
    job2 = Job(
        func=copy_file,
        name='Удалить файл',
        args=(condition, ),
    )
    job3 = Job(
        func=delete_file,
        name='Скопировать файл',
        args=(condition,),
    )
    g = Scheduler()
    g.run((job1, job2, job3))