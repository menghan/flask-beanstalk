import logging

from gevent import Greenlet
import gevent
import beanstalkc

RESERVING = 1
WORKING = 2


class Worker(Greenlet):

    def __init__(self, id, tube='default', job_timeout=60, logger=None, **kwargs):
        Greenlet.__init__(self)
        self.id = id
        self._beanstalk = beanstalkc.Connection(**kwargs)
        if tube != 'default':
            self._beanstalk.ignore('default')
            self._beanstalk.watch(tube)
        self.tube = tube
        self._job_timeout = job_timeout
        if not logger:
            logger = logging.getLogger(repr(self))
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(handler)
        self._logger = logger
        self.keep_running = True
        self.start()

    def __str__(self):
        return 'Worker-%s' % self.id

    @classmethod
    def spawn_workers(cls, count, id_func=lambda x: x, **kwargs):
        return [cls(id_func(x), **kwargs) for x in range(count)]

    @classmethod
    def stop_workers(cls, workers):
        for worker in workers:
            gevent.spawn(worker.stop)
        gevent.joinall(workers)

    def _run(self):
        self._logger.debug('[%s] Started.', self)
        while self.keep_running:
            self.state = RESERVING
            job = self._beanstalk.reserve()
            self.state = WORKING
            try:
                self.work(job)
            except Exception as e:
                self._logger.error('[%s] Uncaught exception: %s', self, e)
                self._logger.exception()
            else:
                job.delete()
        self._logger.debug('[%s] Stopped.', self)

    def work(self, job):
        raise NotImplemented

    def stop(self):
        self._logger.debug('[%s] Stopping...', self)
        self.keep_running = False
        if self.state == WORKING:
            self.join(timeout=self._job_timeout)
        if not self.dead:
            self.kill()
