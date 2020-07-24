from time import sleep
from numpy.random import rand


def do_job(job):
    """Calculates the product of two numbers in a tuple.

    Parameters
    ----------
    job : `tuple` [`int`, `int']
        A tuple containing two integers to be multiplied i.e. (a, b).

    Returns
    -------
    result : `tuple` [`int`, `int`, `int`]
        A tuple of three integers where the first two are those supplied in
        ``job`` followed by a third representing their product.
    """
    sleep(rand())  # <-- slow things down a little
    # Extract the two integers
    a, b = job
    print(f'Calculating the product of {a} and {b}')
    # Calculate their product
    c = a * b
    # Return the result
    return (a, b, c)

if __name__ == '__main__':
    from conman.worker import Worker
    from conman.exceptions import ConmanKillSig
    # Boot & connect the worker it to the coordinator.
    with Worker('', 12346) as worker:  # <-- Blocks until worker's connection is accepted
        result = None  # <-- dummy result
        # Start the main duty cycle, & keep going until the a kill signal is sent
        try:
            while True:
                # In each cycle, a call is made to the worker in which the result of the
                # last job is handed in and a new job is retrieved. As there will be no
                # results to return in the first call, a dummy value of None is sent.
                job = worker(result)
                # Perform the job, and start the cycle again
                result = do_job(job)
        except ConmanKillSig:
            # Kill signal has been sent by the coordinator; terminate the program
            exit()
