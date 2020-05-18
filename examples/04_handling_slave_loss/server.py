if __name__ == '__main__':
    """Send the slaves a random value from 0 to 1."""
    from time import sleep
    from conman.master import Master
    from numpy.random import rand
    with Master('', 12346, max_slave_loss=0) as master:
        master.mount(await_n=2)
        while True:
            # Submit random numbers to the slaves. Values > 0.9 will result in
            # slave termination.
            jobs = [rand(), rand()]
            master(jobs=jobs)
            sleep(0.25)