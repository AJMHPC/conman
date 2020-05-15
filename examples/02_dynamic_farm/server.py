from numpy.random import randint, rand, randint
from time import sleep


def job_god(n):
    """Generator which will periodically yield some dummy jobs for the slaves
    to do in the form of a pair of numbers to multiply together. But only if
    if feels like it.

    Parameters
    ----------
    n : `int`
        The number job jobs to create

    Yields
    -------
    jobs : `list` [`tuple` [`int`, `int`]], None
        A list of tuples, each containing two integers that are to be multiplied
        or None, depending on how the generator is feeling today.
    """
    # Generate some jobs
    jobs = [(randint(100), randint(100)) for i in range(n)]
    # While there are still jobs left to return
    while len(jobs) != 0:
        # Slow things down a little
        sleep(rand())
        # Decide whether or not to return a job this cycle
        if rand() > 0.7:
            # Return 1-6 jobs
            yield [jobs.pop(0) for i in range(randint(1,7)) if len(jobs) != 0]
        else:
            # Looks like they get nothing this time
            yield None

def print_results(results):
    """Print out the results.

    Parameters
    ----------
    results : `list` [`tuple` [`int`, `int`, `int`]]
        Results returned by the slave.

    Notes
    -----
    This operation is abstracted to simplify the example.
    """
    # Print out the results
    for a, b, c in results:
        print(f'\t{a} * {b} = {c}')

if __name__ == '__main__':
    from conman.master import Master
    # Create a master and bind it to the host and port
    with Master('', 12346) as master:
        # Mount the salves
        master.mount(await_n=2)
        print('The following jobs were completed:')
        # The job god will randomly yield 0-6 jobs.
        for jobs in job_god(30):
            # Submit jobs & get results that were waiting or finished quickly.
            results = master(jobs=jobs)
            # Print any results
            if results is not None:
                print_results(results)
        # Get remaining results via await_results. Alternatively master() can be
        # called until there are no more jobs running or results waiting to be
        # returned i.e if ``master.active``.
        results = master.await_results()
        print_results(results)