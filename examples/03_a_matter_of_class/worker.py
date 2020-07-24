from server import TestClass

if __name__ == '__main__':
    """Simple script to take a class, run one of its functions & return it."""
    from conman.worker import Worker
    from conman.exceptions import ConmanKillSig
    with Worker('', 12345) as worker:
        job = None
        try:
            while True:
                job = worker(job)
                job.do_stuff()
        except ConmanKillSig:
            exit()
