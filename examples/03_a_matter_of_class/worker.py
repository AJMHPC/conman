from server import TestClass

if __name__ == '__main__':
    """simple script to take a class, run one of its functions & return it."""
    from conman.slave import Slave
    from conman.exceptions import ConmanKillSig
    with Slave('', 12345) as slave:
        job = None
        try:
            while True:
                job = slave(job)
                job.do_stuff()
        except ConmanKillSig:
            exit()
