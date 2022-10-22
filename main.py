import asyncio

from utils.job import Job
from utils.scheduler import Scheduler


async def ather_task(time: int):
    await asyncio.sleep(time)


async def main(scheduler: Scheduler, jobs: list[Job]):
    [await scheduler.put(job) for job in jobs]
    await scheduler.recovery_queue()
    scheduler.start()
    await scheduler.join()
    await scheduler.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    s = Scheduler()
    tasks = []
    # В данном блоке приведен пример создания задач, несколько кейсов также реализовано в тестах.
    # Экземпляр Job принимает параметры: tid - идентификатор задачи, coroutine - сама корутина
    # timeout - максимальное время исполнения задачи, max_retry - число ретраев при ошибке,
    # kwargs - дикт с аргументами для корутины, start_at - время отложенного запуска в формате '21-10-2022 20:28',
    # dependence - tid таски, от которой зависит задача.

    for task_id in range(5):
        job = Job(tid=task_id, coroutine=ather_task, timeout=3, max_retry=3, kwargs={'time': 2})
        tasks.append(job)

    try:
        loop.run_until_complete(main(s, tasks))
    except KeyboardInterrupt:
        # вызываем метод stop при ручной остановке планировщика, для сохранения не исполненных тасок
        loop.run_until_complete(s.stop())
        loop.close()
