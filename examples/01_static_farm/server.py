from numpy.random import randint

def make_jobs(n):
    """Creates some dummy jobs for the slaves to do in the form of a pair of
    numbers to multiply together.

    Parameters
    ----------
    n : `int`
        The number job jobs to create

    Returns
    -------
    jobs : `list` [`tuple` [`int`, `int`]]
        A list of tuples, each containing two integers that are to be multiplied.

    Notes
    -----
    This operation is abstracted to simplify the example.
    """
    # Generate some jobs
    jobs = [(randint(100), randint(100)) for i in range(n)]
    # Return the jobs
    return jobs


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
    print('The following jobs were completed:')
    for a, b, c in results:
        print(f'\t{a} * {b} = {c}')


if __name__ == '__main__':
    from conman.master import Master
    # Connection settings
    host = ''  # <-- machine host server on ('' means "this machine")
    port = 12348  # <-- port to listen to
    # Create a master and bind it to the host and port
    with Master(host, port) as master:
        # Mount is called to accept any slaves attempting to connect. The ``await_n``
        # keyword can be used to force the master to wait for the specified number
        # of slaves to connect.
        master.mount(await_n=2)  # <-- Start this many slave scripts
        # Generate some jobs for the slaves to do
        jobs = make_jobs(10)
        # Farm out all of the jobs at once, but set fetch=False so that some of
        # the results are not automatically returned.
        master(jobs=jobs, fetch=False)
        # Wait for all of the jobs to finish
        results = master.await_results()  # <-- Blocks until all jobs are done
        # Print out the results (post-processing placeholder function)
        print_results(results)
        # When using multiple slaves, the order in which results are returned
        # does not always match the order in which the jobs were sent.
    # Once the above context closes a Kill signal will be sent to the slaves.
