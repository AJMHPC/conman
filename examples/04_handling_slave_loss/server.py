if __name__ == '__main__':
    """Send the workers a random value from 0 to 1."""
    from time import sleep
    from conman.coordinator import Coordinator
    from numpy.random import rand
    with Coordinator('', 12346, max_worker_loss=0) as coordinator:
        coordinator.mount(await_n=2)
        while True:
            # Submit random numbers to the workers. Values > 0.9 will result in
            # worker termination.
            jobs = [rand(), rand()]
            coordinator(jobs=jobs)
            sleep(0.25)