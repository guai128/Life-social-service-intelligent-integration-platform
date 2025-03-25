import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler


class DelayScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def add_task(self, callback, args, delay):
        run_time = time.time() + delay / 1000
        f_time = datetime.fromtimestamp(run_time).strftime('%Y-%m-%d %H:%M:%S')

        return self.scheduler.add_job(func=callback, trigger='date', run_date=f_time , args=args)

    def remove_task(self, job_id):
        self.scheduler.remove_job(job_id=job_id)


def func(text: str):
    print(f'hello {text}')


if __name__ == '__main__':
    delayS = DelayScheduler()
    delayS.add_task(func, ['wubin'], 2000)
    time.sleep(10)
