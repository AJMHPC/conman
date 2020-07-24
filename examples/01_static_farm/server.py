from numpy.random import randint

def make_jobs(n):
    """Creates some dummy jobs for the workers to do in the form of a pair of
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
        Results returned by the worker.

    Notes
    -----
    This operation is abstracted to simplify the example.
    """
    # Print out the results
    print('The following jobs were completed:')
    for a, b, c in results:
        print(f'\t{a} * {b} = {c}')


if __name__ == '__main__':
    from conman.coordinator import Coordinator
    # Connection settings
    host = ''  # <-- machine host server on ('' means "this machine")
    port = 12348  # <-- port to listen to
    # Create a coordinator and bind it to the host and port
    with Coordinator(host, port) as coordinator:
        # Mount is called to accept any workers attempting to connect. The ``await_n``
        # keyword can be used to force the coordinator to wait for the specified number
        # of workers to connect.
        coordinator.mount(await_n=2)  # <-- Start this many worker scripts
        # Generate some jobs for the workers to do
        jobs = make_jobs(10)
        # Farm out all of the jobs at once, but set fetch=False so that some of
        # the results are not automatically returned.
        coordinator(jobs=jobs, fetch=False)
        # Wait for all of the jobs to finish
        results = coordinator.await_results()  # <-- Blocks until all jobs are done
        # Print out the results (post-processing placeholder function)
        print_results(results)
        # When using multiple workers, the order in which results are returned
        # does not always match the order in which the jobs were sent.
    # Once the above context closes a Kill signal will be sent to the workers.
