from conman import Conman
from exceptions import ConmanKillSig


class Slave:
    """A reactive entity that's purpose is to receive a command over a socket
    connection, perform some action based on that command, and return a result
    based on that action. Multiple slave instances are to be used in tandem with
    a single master instance to permit task farming.

    Parameters
    ----------
    host : `str`
        Host to connect to.
    port : `int`
        Port to establish connection through.
    **kwargs
        Additional settings may be changes using the various keyword arguments
        described below:

        ``timeout``:
            Time in seconds to keep attempting to connect with the superior before
            raising an error (`float`, `int`).
    """
    def __init__(self, host, port, **kwargs):
        self.soc = Conman((host, port))

        self.timeout = kwargs.get('timeout', 60)

        # Allows for one call to __call__ to be made without an argument
        self.__free_pass = True

    def connect(self):
        """Connect the slave to its superior.

        Notes
        -----
        Calling this function prior to master instantiation will likely result
        in a ConnectionRefusedError being raised.
        """

        # Attempt to establish a connection to the master, try for at least
        # ``timeout`` seconds before giving up.
        self.soc.make_connection(self.timeout)

    def disconnect(self):
        """Ensure the connection is terminated gracefully upon exit.
        """
        # Kill the connection
        self.soc.kill()

    def __enter__(self):
        """Upon entry a connection will be established to a superior, i.e.
        a Master or SlaveDriver.

        Returns
        -------
        self : `Slave`
            Connected slave instance.

        Notes
        -----
        Calling this function prior to master instantiation will likely result
        in a ConnectionRefusedError being raised.
        """
        # Attempt to establish a connection to the master
        self.connect()

        # Return self
        return self

    def __exit__(self, exc_type, exc_value, exc_trace):
        """Disconnect from the superior.
        """
        # If no exception was raised
        if exc_type is None:
            # Then shutdown normally
            self.disconnect()
        # If a kill signal was sent from the master, then the socket has already
        # been closed so there is no need to call disconnect.
        elif exc_type is ConmanKillSig:
            pass

    def __call__(self, result):
        """This call is used by the slave to send the results of the last job
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
        A slave can only be in possession of one job at a time and cannot receive
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
