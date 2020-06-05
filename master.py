import tempfile
from socket import CMSG_SPACE
from time import sleep, time

from conman.exceptions import ConmanIncompleteMessage, ConmanMaxSlaveLoss, ConmanNoSlavesFound
from conman.utils import save_to_page, load_from_page

from conman.conman import Conjour

"""
TODO:
    - Look at implementing a master poll list and associating the file numbers to
        enable quick location of returned results.
    - Add method to deal with the "poisoned job" effect.     
    - Turn the load_page operation into a generator to prevent loading lots of
        stuff into memory at once. Especially if it may be immediately placed back.  
    - Abstract type checking to an external wrapper.
    - Add class properties to the class's doc-string.
    - Consider renaming and reworking the "handshake" parameter and improve
        its documentation.
"""

class Master:
    """Manages job distribution and result gathering operations for multiple
    slave connections.

    Parameters
    ----------
    host : `str`
        Name or IP address of the device on which to open a socket.
    port : `int`
        Port number on which to listen for connections.
    handshake : `bool`, optional
        By default version compatibility is ensured through the use of a
        handshake message. However, if it is known that the master and all
        slaves use the same protocol versions then this can be safely turned
        off to give reasonable speed up. [DEFAULT=True]

    **kwargs

        ``max_slave_loss``:
            Specifies the maximum number of lost slaves that will be tolerated
            before a ConmanMaxSlaveLoss exception is raised. A lost slave is
            defined as one that can no longer be reached via its socket
            connection, i.e. it has crashed (`bool`).
        ``no_slave_kill``:
            If no_slave_kill is set to True then a ConmanNoSlavesFound exception
            will be raised if all slaves have been lost. Even if that number is
            technically less than the ``max_slave_loss`` value. [DEFAULT=True]

    Properties
    ----------
    soc : `Conjour`
        Master socket entity.
    slaves : `list` [`Conjour`]
        List to hold the slave socket connections.
    _job_page : `tuple` [`TemporaryFile`, `list` [`int`]]
        A temporary file to hold jobs that cannot be submitted yet due to
        insufficient resource availability and a list of indices specifying
        the length in bytes of each entry in said file.
    _res_page : tuple` [`TemporaryFile`, `list` [`int`]]
        The same as ``_job_page`` but designed to hold results rather than jobs.
    _lost_slave_count : `int`
        A counter for the number of lost slaves.
    _await_time : `float`, `int`
        Time in seconds to wait between submission attempts. Use will be extended
        to other functions later.
    """

    def __init__(self, host, port, handshake=True, **kwargs):
        self.soc = Conjour((host, port), handshake=handshake)

        self.compress = kwargs.get('compress', False)

        # List to hold slave socket connections
        self.slaves = []

        self.handshake = handshake

        # Slave loss behaviour
        self.max_slave_loss = kwargs.get('max_slave_loss', 2)
        self.no_slave_kill = kwargs.get('no_slave_kill', True)
        self._lost_slave_count = 0

        # Temporary files and journals for paging results and jobs to.
        self._job_page = (tempfile.TemporaryFile(buffering=0), [])
        self._res_page = (tempfile.TemporaryFile(buffering=0), [])

        self._await_time = 0.25

    @property
    def active(self):
        """Returns True if there are still jobs running, waiting to run or
        there are results to be returned.

        Returns
        -------
        activity_status :  `bool`
            True if there are still active jobs or pending results, False if not.
        """
        return len(self.idle_slaves) != 0 or self._paged_jobs or self._paged_results

    @property
    def idle_slaves(self):
        """Returns a list of idle slaves.

        Returns
        -------
        idle_slaves : `list` [`Conjour`]
            List of slaves currently sitting idle
        """
        return [slave for slave in self.slaves if slave.idle]

    @property
    def _paged_results(self):
        """Returns True if there is paged results data.

        Returns
        -------
        paged_data : `bool`
            Bool indicating the presence of paged results data
        """
        # If there is paged data, there will be a journal entry. So just check
        # if the number of journal entries is zero or not.
        return len(self._res_page[1]) != 0

    @property
    def _paged_jobs(self):
        """Returns True if there is paged job data.

        Returns
        -------
        paged_data : `bool`
            Bool indicating the presence of paged job data
        """
        # If there is paged data, there will be a journal entry. So just check
        # if the number of journal entries is zero or not.
        return len(self._job_page[1]) != 0

    def mount(self, await_n=None, timeout=None):
        """Check for & accept pending connections, slaves not yet present in the
        connection queue will be missed in a standard call. Thus it is advised
        to use the await_n option to force blocking until the target number of
        slaves have been mounted. If required, a timeout can also be set to
        enable escape of the await cycle. [DEFAULT:non-blocking]

        Parameters
        ----------
        `await_n` : `int`, optional
            If specified then the function will block until the target number
            of slaves have been mounted. [DEFAULT=None]property
        `timeout` : `float`, `int`, `None`, optional
            Places an upper bound, in seconds, on the amount of time that this
            function blocks for when await_n is specified. This is intended to
            allow the user to await for connections but abort if it looks like
            something is wrong. If `None` is specified then the await operation
            will block forever. [DEFAULT = None]

        Notes
        -----
        await_n is just the **minimum** number of slaves to await on so it is
        still possible to mount more than ``await_n`` number of slaves.
        """
        # Ensure await_n is not set to zero, as it would cause timeout to be ignored
        if await_n <= 0:
            raise ValueError('"await_n" must be None or a none zero positive integer')

        # If await_n is specified; keep checking for slaves until `timeout` second
        # have elapsed. If timeout is None, then continue checking forever.
        poll_time_out = timeout if await_n else 0

        # If await_n is None: set it to zero to make breaking while loop easy
        await_n = await_n if await_n else 0

        # While there are pending connections
        while self.soc.poll(poll_time_out):
            # Accept the next connection & add the slave to the slave list
            self.slaves.append(self.soc.accept_connection())
            # If no connections in the queue & the specified number of slaves
            # have been mounted.
            if not self.soc.poll(0) and len(self.slaves) >= await_n:
                # End the mounting process
                break

    def submit(self, jobs):
        """Farms out supplied jobs to free slaves.

        Parameters
        ----------
        jobs : `list`
            List of jobs to be submitted.
        """
        if type(jobs) != list:
            raise TypeError('Jobs must be supplied in a list')
        # If self.handshake = False: All jobs will be packed in the same way,
        # thus pack all jobs ahead of time to speed things up. Note that it
        # does not matter which slave does the packing as they will all do it
        # the same way.
        if not self.handshake:
            jobs = [self.slaves[0].pack(job, compress=self.compress) for job in jobs]
        else:
            # Otherwise; clone the jobs list so the original is not modified
            jobs = jobs.copy()
        # In an effort to free up slaves prior to job submission an attempt is
        # made to pre-fetch and store pending results
        self.retrieve(to_page=True)
        # Load any previously paged jobs, don't unpickle if handshake=False
        if self._paged_jobs:
            jobs += load_from_page(*self._job_page, unpickle=self.handshake)
        # While there are idle slaves and jobs left to submit
        while self.idle_slaves and jobs:
            # Loop over any idle slaves and pair them with a job
            for slave, job in zip(self.idle_slaves, jobs):
                # Submit the job to the slave, the job will have been pre-packed
                # if handshake=False
                slave.send_message(job, packed=not self.handshake, compress=self.compress)
                # Remove the job form the job list
                jobs.remove(job)
            # repeat the paging process
            self.retrieve(to_page=True)
        if jobs:
            # Create a list slaves list ranked by free port buffer space.
            slaves = sorted(self.slaves, key=lambda s: -s.free_space)
            # Loop over all remaining job
            for job in jobs:
                # Loop over all slaves
                for slave in slaves:
                    # Pack the job, to calculate its size. It if fits into the
                    # slave's port buffer then submit it. It will already have
                    # been packed if handshake=False
                    if self.handshake:
                        packed_job = slave.pack(job)
                    else:
                        packed_job = job
                    if CMSG_SPACE(len(packed_job)) < slave.free_space:
                        # Submitting the packed job as it is more efficient
                        slave.send_message(packed_job, packed=True)
                        # Remove the job from the jobs list
                        jobs.remove(job)
                        # Move this slave to the back of the slaves list
                        slaves = slaves[1:] + slaves[:1]
                        # Break the slave loop
                        break
        # If there are jobs left that could not be submitted
        if jobs:
            # Then page them for submission later on. If handshake=False then
            # don't pickle as they will have already been pickled.
            save_to_page(jobs, *self._job_page, as_pickle=self.handshake)


    def retrieve(self, to_page=False):
        """Checks for and returns any pending results received from the slaves.

        Returns
        -------
        results : `list` [`serialisable']
            List of results returned by slaves. If no results have been found
            then an empty list will be returned.
        to_page : `bool`, optional
            If set to True, then all results are automatically saved to the page
            file and nothing is returned. [DEFAULT=False]

        Notes
        -----
        Due to the way in which a test for a broken TCP connection must be
        performed (i.e a check for writable data on an empty buffer) it is
        most effective when performed just before a read. Therefore, the test
        for lost slaves is done in this function.
        """
        # Creat a list to hold the results
        results = []

        # If there are any paged results then add them to the results list, but
        # only do this if not saving the results to the page file.
        if self._paged_results and not to_page:
            results += load_from_page(*self._res_page)

        # Shortcut for results.append to reduce loop overhead
        add_to_results = results.append

        # Loop over the slaves
        for slave in self.slaves:
            # While the slave as data available to read
            while slave.poll():
                # Check that the slave is a alive
                if slave.alive:
                    # If it is read & append the message to the results list
                    try:
                        # Use a timeout of 10 seconds to catch incomplete messages
                        add_to_results(slave.await_message(timeout=10))
                    except ConmanIncompleteMessage:
                        # The presence of an incomplete message indicates that
                        # the code on the other end crashed during a send
                        # operation, thus this slave must be purged.
                        self._purge_lost_slave(slave)
                        break
                else:
                    # If this slave is dead then it must be purged
                    self._purge_lost_slave(slave)
                    # Break out of the polling loop
                    break

        # If instructed so save the results to a page file
        if to_page:
            save_to_page(results, *self._res_page)
            return None
        # Otherwise return the results
        else:
            return results

    def await_results(self):
        """This will continue gathering results until all slaves are idle. At
        which point the results will be returned.

        Returns
        -------
        results : `list` [`serialisable`]
            List containing the results from of all outstanding jobs.
        """
        # This function is comprised of two loops; 'sub_loop' submits paged
        # jobs while 'fetch_loop' retries results until all have been returned.
        # The two operations have been functionalised to make dealing with slave
        # loss easier. However, it must be noted that this can 1) in result in
        # a "poisoned" job been passed from one slave to the next killing all
        # in its path (e.g. def x(): exit()), 2) under some (admittedly unlikely)
        # conditions result in the recursion limit being breached by
        # fetch_loop()-sub_loop() calls, and 3) be rather inefficient.

        def fetch_loop():
            # While slave are active
            while [s for s in self.slaves if not s.idle]:
                # Fetch any new results, but don't load those in the page, and save
                # them to the page
                self.retrieve(to_page=True)
                sleep(self._await_time)
            # Check that no more jobs need to be submitted due to slave loss
            if self._paged_jobs:
                # If so call back to sub_loop
                sub_loop()

        def sub_loop():
            # Continue looping while there are still jobs to run
            while self._paged_jobs:
                # Try submitting them (they are loaded within the submit function
                # so a blank list is passed here)
                self.submit([])
                # Wait a few seconds before before trying again
                sleep(self._await_time)
            # Once all jobs have been submitted start fetching jobs
            fetch_loop()

        # Start the process off by submitting any paged jobs.
        sub_loop()

        # Load all results from the page file and return them
        return load_from_page(*self._res_page)

    def _purge_lost_slave(self, lost_slave):
        """Removes lost a lost slave from the slaves list, reassigns its jobs
        and shuts it down.

        Parameters
        ----------
        lost_slave : `Conjour`, `Conman`
            The lost slave to that is to be purged.
        """
        # Remove the lost_slave from the slaves list
        self.slaves.remove(lost_slave)
        # Reassign any jobs that were lost with the slave. First read the message
        # from the slave's own page file.
        jobs = load_from_page(*lost_slave.journal, unpickle=False)
        # If handshake mode is enabled then the messages will need to be unpacked
        if self.handshake:
            jobs = [lost_slave.unpack(job)[0] for job in jobs]
        # Save the jobs to the page, don't pickle if not needed
        save_to_page(jobs, *self._job_page, as_pickle=self.handshake)
        # Kill the slave
        lost_slave.kill()
        # Increment the lost slave counter
        self._lost_slave_count += 1

    def disconnect(self,):
        """Ensure the connection is terminated gracefully upon exit.
        """
        # Loop over the slaves and then shut down
        for slave in self.slaves:
            # Send kill command
            slave.send_message('CONMAN_KILL', command=True)
            slave.kill()
        # Close the page files
        self._job_page[0].close()
        self._res_page[0].close()

    def __call__(self, jobs=None, fetch=True):
        """Farms out any supplied jobs and returns the results of any complected
        ones.

        Parameters
        ----------
        jobs : `list` [`serialisable`], `None`, optional
            List of jobs to be submitted.
        fetch : `bool`, optional
            Specifies if results from past jobs should be returned. [DEFAULT=True]

        Returns
        -------
        results : `list` [`serialisable`]
            Results returned from past jobs; only returned when ``fetch`` is True.
        """
        # Submit any supplied jobs
        if jobs:
            self.submit(jobs)
        # Check if the number of casualties has reached the specified threshold
        if self._lost_slave_count > self.max_slave_loss:
            raise ConmanMaxSlaveLoss(
                'Maximum number of lost slaves has been surpassed'
                f' ({self._lost_slave_count})')
        # Test if all slaves have been lost
        elif self._lost_slave_count != 0 and len(self.slaves) == 0:
            # If so raise a ConmanNoSlavesFound error, but only if
            # no_slave_kill is set to True.
            if self.no_slave_kill:
                raise ConmanNoSlavesFound('All slaves have been lost')
        # Fetch and return the results of any complected ones if told to
        if fetch:
            return self.retrieve()

    def __enter__(self):
        """Entry function for context manager.

        Returns
        -------
        self : `Master`
            Returns self

        Notes
        -----
        This will not establish slave connections. This must be done via the
        ``mount`` command.
        """
        # Return self
        return self

    def __exit__(self, *args):
        """Upon exiting ensure that slave connections are terminated and page
        files are closed
        """
        # Close connections and page files
        self.disconnect()
