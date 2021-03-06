from conman.exceptions import ConmanKillSig

from conman.conman import Conman
from time import time

"""
TODO:
    - rename the "handshake" parameter and improve its documentation. Is it even
        needed.

"""

class Worker:
    """A reactive entity that's purpose is to receive a command over a socket
    connection, perform some action based on that command, and return a result
    based on that action. Multiple worker instances are to be used in tandem with
    a single coordinator instance to permit task farming.

    Parameters
    ----------
    host : `str`
        Host to connect to.
    port : `int`
        Port to establish connection through.
    handshake : `bool`, optional
        By default version compatibility is ensured through the use of a
        handshake message. However, if it is known that the coordinator and all
        workers use the same protocol versions then this can be safely turned
        off to give reasonable speed up. [DEFAULT=True]
    **kwargs
        Additional settings may be changes using the various keyword arguments
        described below:

        ``timeout``:
            Time in seconds to keep attempting to connect with the superior before
            raising an error (`float`, `int`).

    """
    def __init__(self, host, port, handshake=True, **kwargs):
        self.soc = Conman((host, port), handshake=handshake)

        self.timeout = kwargs.get('timeout', 60)
        self.handshake = handshake

        # Allows for one call to __call__ to be made without an argument
        self.__free_pass = True


    def connect(self):
        """Connect the worker to its superior.

        Notes
        -----
        Calling this function prior to coordinator instantiation will likely result
        in a ConnectionRefusedError being raised.
        """

        # Attempt to establish a connection to the coordinator, try for at least
        # ``timeout`` seconds before giving up.
        self.soc.make_connection(self.timeout)

    def disconnect(self):
        """Ensure the connection is terminated gracefully upon exit.
        """
        # Kill the connection
        self.soc.kill()

    def __enter__(self):
        """Upon entry a connection will be established to a superior, i.e.
        a Coordinator.

        Returns
        -------
        self : `Worker`
            Connected worker instance.

        Notes
        -----
        Calling this function prior to coordinator instantiation will likely result
        in a ConnectionRefusedError being raised.
        """
        # Attempt to establish a connection to the coordinator
        self.connect()

        # Return self
        return self

    def __exit__(self, exc_type, exc_value, exc_trace):
        """Disconnect from the superior.
        """
        # Disconnect
        self.disconnect()

    def __call__(self, result):
        """This call is used by the worker to send the results of the last job
        back to its superior and to get a new task.

        Parameters
        ----------
        result : `serialisable`
            Results of the last job which are to be sent to the superior. In the
            first call to this function the ``result`` parameter will not be sent
            to the superior, thus None should be supplied.

        Returns
        -------
        job : `Any`
            A message from a superior detailing a job to be carried out.

        Notes
        -----
        A worker can only be in possession of one job at a time and cannot receive
        another until the results of the last one have been sent back.
         |
        As a job can take the form of any picklable entity, it is up to the
        user to interpret what must be done and what should be sent back as
        a result.
        """

        # In the first call to this function ``result`` will not sent to the
        # superior. Thus, just get and return a job.
        if self.__free_pass:
            # Block further free passes
            self.__free_pass = False
            # If data was supplied during the first pass
            if result is not None:
                # Raise an error as this data will not be sent when the user may
                # expect it to be.
                raise Exception('"None" must be supplied to the first function call')
            # Fetch and return a result
            return self.soc.await_message()

        # If this is a standard call:
        # Send the result form the last job
        self.soc.send_message(result)
        # Retrieve and return a new job
        return self.soc.await_message()
