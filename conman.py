import bz2
import pickle
import select
import struct
import tempfile
from _socket import dup
from socket import socket, AF_INET, SOCK_STREAM, SO_RCVBUF, SOL_SOCKET, CMSG_SPACE, MSG_PEEK
from time import time, sleep

from conman.exceptions import ConmanKillSig, ConmanIncompleteMessage
from conman.utils import save_to_page, load_from_page

"""
TODO:
    - Work on introducing a global packing process to speed up operations.
    
    - Swap out CONMAN_XXX type variables for integer values with global
        labels.
"""

class Conman(socket):
    """This is a connection manager that augments TCP based sockets to introduce
    higher-level functionality in the form of on-the-fly datagram construction
    allowing for the transition of compressed, pickled python objects.

    Parameters
    ----------
    address : `tuple` [`str`, `int`]
         Host name and port to which data is to be sent.

    Properties
    ----------
    PROTO : `dict`
        Specifies protocol version information, i.e. what pickle protocol
        version is to be used, etc. These should be initialized to the most
        widely adopted versions. Version numbers will be updated to highest
        mutually available values during the handshake operation.
    _poll : `select.poll`
        Used to identify when readable data is present in the port buffer.
        This is needed as there is no other way to check, and reading when
        there is no data will cause a block until there is data.
    _RCVBUF : `float`
        The size in bytes of the port receive buffer. Used to identify how
        much information can be received outside of the ``_read_message``
        function before the sender starts to block.
    _SNDBUF : `float`
        Same as _RCVBUF but for the other end of the connection, i.e the
        entity that this socket is connected to. Used to identify how much
        data can be sent before sending becomes a blocking operation. It
        should be noted that this is different to the true SNDBUF as defined
        by socket options.
    _is_server: `bool`
        Used behind the scenes to identity if the conman instance is on the
        server/master (True) side or the client/slave side (False).

    """
    def __init__(self, address, *args, **kwargs):

        # Initialise socket with standard address family & connection type. The
        # proto & fileno arguments are included to aid conversions functions.
        super().__init__(AF_INET, SOCK_STREAM, kwargs.get('proto', 0),
                         kwargs.get('fileno', None))

        self.address = address

        self.PROTO = {'PICKLE': 3, 'CONMAN': 1}
        self._poll = select.epoll()

        self._RCVBUF = 0.
        self._SNDBUF = 0.

        self.__setup()

        self._is_server = False

    def __setup(self):
        """This finishes the initialisation process.
        """
        # Register with the poll to monitor for readable data in the socket's
        # port buffer.
        self._poll.register(self, select.POLLIN)

        # Increase receive buffer's size to the larges system permitted value.
        # As the system limit is quite small it is okey to max it out. Note
        # that this size is limited by the network switch, not system memory.
        self.setsockopt(SOL_SOCKET, SO_RCVBUF, struct.pack('Q', int(1E10)))

        # Record the receive buffer's size
        self._RCVBUF = self.getsockopt(SOL_SOCKET, SO_RCVBUF)

    # <MESSAGING_CODE>
    def send_message(self, message, **kwargs):
        """Packs up and sends a message to the connected socket.

        Parameters
        ----------
        message : `serialisable`
             Data to be sent
        **kwargs
            ``pkld``:
                Use to indicate that a message has already been pickled, & forces
                the Pickled flag to True while treating it as a bytes object.
            ``command``:
                Flag used to send command and control messages. [DEFAULT=False]
            ``packed``:
                Flag used to indicate that a message has already been packed.
                [DEFAULT=False]
            ``compress``:
                Compress message prior to sending. [DEFAULT=True]

        Notes
        -----
        Function will continue to block until the entire message has been sent.
        Blocking is encountered when the message size exceeds the available free
        space in the target's port buffer and said buffer is not cleared quickly.
        """

        # Parse the message into a byte-stream and package it into a data-frame,
        # but only if it is not already packed
        if not kwargs.get('packed', False):
            message = self.pack(message, **kwargs)

        # Send the message
        self.send(message)

    def _read_message(self):
        """Backend code used by ``await_message`` to read and unpack messages.

        Returns
        -------
        message : `serialisable`, `str`, `bytes`
            Data received from the connected socket.

        Notes
        -----
        This will automatically execute any command and control messages encountered.
        """
        # Get the data stream's first 8 bytes to determine message length and
        # ensure retrial of the full 8 bytes.
        size_bytes = self.recv(8)
        while len(size_bytes) < 8:
            new_bytes = self.recv(8 - len(size_bytes))
            # Catch for timeout related errors
            if len(new_bytes) == 0:
                # Likelihood of this being encountered is very low
                raise ConmanIncompleteMessage('Cannot fetch length header')
            size_bytes += new_bytes

        # Convert these bytes, which represent an unsigned long int, into an int
        message_size, = struct.unpack('L', size_bytes)

        # Read from the socket stream until the full message has been read
        message_bytes = self.recv(min(message_size, 4096))
        while len(message_bytes) < message_size:
            # Maximum amount of data to be read from the buffer at any one time
            # is capped at 4096 bytes. This slows things down but is more stable
            new_bytes = self.recv(min(message_size - len(message_bytes), 4096))
            if len(new_bytes) == 0:
                # It is possible, but unlikely for this to happen with a
                # sensible timeout value.
                raise ConmanIncompleteMessage(
                    f'Incomplete message received{len(message_bytes)} of'
                    f' {message_bytes} bytes received')
            message_bytes += new_bytes

        message, command = self.unpack(message_bytes, length_prefix=False)

        # If the message is a command
        if command:
            # Then pass the command to the system
            self._interpret_command(message)
            # Then repeat the read operation to get a user message
            message = self._read_message()

        # Return the message
        return message

    def await_message(self, **kwargs):
        """Waits until a message is received, unpacks it & returns its content.

        Returns
        -------
        message : `serialisable`, `str`, `bytes`
            Data received from the connected socket.
        **kwargs
            ``timeout``:
                Flag used to specify the time after which the await should be
                aborted and an exception raised. Very small values may cause
                unpredictable results.

        Notes
        ----
        This function will block until a complete message is received.
         |
        This function is used, rather than a direct call to ``_read_message``
        to avoid code reaction and allow flexibility in child classes.
        """
        # If the timeout is specified then set a timeout value for the duration
        # of this function.
        if 'timeout' in kwargs:
            self.settimeout(kwargs['timeout'])
        # Read the new message
        message = self._read_message()
        # Reset blocking status
        if 'timeout' in kwargs:
            self.setblocking(True)
        # Return the message
        return message

    def pack(self, message, **kwargs):
        """Packs message data into a form that can be easily sent & interpreted.

        Parameters
        ----------
        message : `serialisable`
             Data to be sent
        **kwargs
            ``pkld``:
                Use to indicate that a message has already been pickled, & forces
                the Pickled flag to True while treating it as a bytes object
                (`bool`). [DEFAULT=False]
            ``command``:
                Flag used to send command and control messages (`bool`).
                [DEFAULT=False]
            ``compress``:
                Compress message prior to sending (`bool`). [DEFAULT=False]

        Returns
        -------
        packed_message : `bytes'
            The packed message with header as a bytes object.

        Notes
        -----
        A packed message is a bytes object comprised of a header followed by
        the message data. The header takes the form:

        .. _table-label:

            +-------+---------+--------------+
            | Bytes | Type    | Name         |
            +=======+=========+==============+
            | 8     | ULong   | Message_size |
            +-------+---------+--------------+
            | 1     | Bool    | Command      |
            +-------+---------+--------------+
            | 1     | Bool    | Compressed   |
            +-------+---------+--------------+
            | 1     | Bool    | Pickled      |
            +-------+---------+--------------+
            | 1     | Bool    | String       |
            +-------+---------+--------------+
            | n     | bytes   | Message_data |
            +-------+---------+--------------+

        Where:
            - Message_size: Total length of packed message excluding Message_size.
            - Command: Indicates if the message contents are a command intended
                for the conman or a message for the user.
            - Compressed: Indicates if Message_data is compressed.
            - Pickled: Indicates if Message_data is a pickled object.
            - String: Indicates if the message data is a string (True) or bytes
                (False) entity, only relevant to non-pickled entities.
            - Message_data: The message that is to be send.

        |

        If the message data is comprised of a single bytearray it will be
        interpreted as a bytes object upon reception.
        """
        # Initialise header values to default / dummy values
        is_string = False
        is_pickled = False
        compress = kwargs.get('compress', True)

        # Identify message as bytes, string or other: set header values
        # then perform any other instance specific operations as needed.
        if isinstance(message, (bytes, bytearray)):  # <-- If bytes or bytearray
            pass
        # If it is a string
        elif isinstance(message, str):  # <-- if string
            is_string = True
            # Encode the message string as a bytes entity
            message = message.encode('utf-8')
        else:  # <-- Anything else gets pickled
            is_pickled = True
            # Pickle the message entity
            message = pickle.dumps(message, protocol=self.PROTO['PICKLE'])

        # Compress the message unless instructed to do so
        if compress:
            message = bz2.compress(message)

        # Construct the header
        header = struct.pack('L????',
                             # Length of message + header (excl. Message_size)
                             len(message) + 4,
                             # Command status
                             kwargs.get('command', False),
                             # Compression status
                             compress,
                             # Pickling status
                             kwargs.get('pkld', is_pickled),
                             # String or bytes object (if not pickled)
                             is_string)

        # Pack the message and return it
        return header + message

    def unpack(self, message, length_prefix=True):
        """Unpacks a message to yield its contents.

        Parameters
        ----------
        message : `bytes`
            A full, packed message in bytes without the first 8 byte length prefix.

        Returns
        -------
        message_data : `serialisable`
            The unpacked message data.
        command : `bool`
            A boolean indicating if this is a command message.
        """
        # The first 4 bits indicate message's command, compression, pickled and
        # string status. See conman.pack documentation for more info.
        command, compressed, pickled, string = struct.unpack('????', message[0:4])
        message = message[4:]
        # Decompress the message if required
        if compressed:
            message = bz2.decompress(message)
        # Unpickle the message if required
        if pickled:
            message = pickle.loads(message)
        # If the message is not a pickled object but a string
        elif string:
            message = message.decode('utf-8')
        # If not pickled and not a string then leave it as bytes

        # Return the message and command status
        return message, command

    # </MESSAGING_CODE>

    def poll(self, timeout=0):
        """Indicates the present of readable socket data. Will be true if data
        is in the port's receive buffer or there is a pending connection.

        Parameters
        ----------
        timeout : `int`, `float`, `None`, optional
            Time in seconds to continue checking for readable data if none is
            found initially. If timeout = 'None' then poll will block until
            readable data is present. If time = 0 then a single check without
            waiting is performed. [DEFAULT=0]

        Returns
        -------
        readable_data_present : `bool`
            A boolean indicating the presence of readable data.
        """

        # Multiply the time by 1000 as poll expects time in ms, but don't
        # multiply if timeout is None. If timeout = zero then use a very
        # small timeout value: this is done as poll() will internally convert
        # 0 to None.
        timeout = timeout * 1000 if timeout else 1E-5 if timeout == 0 else None
        # When select.poll.poll() ends it returns a list of all registered
        # entities that have readable data. Thus just check if the length of
        # the list is zero or not.
        return len(self._poll.poll(timeout)) != 0

    def _interpret_command(self, command):
        """Carries out instruction based on the command received

        Parameters
        ----------
        command : `str`
            The command to be carried out this may be of one of the following:
                - CONMAN_KILL: Indicates that the connection is to be terminated
                    via the use of an exception.
        """
        # If the kill command is given
        if command == 'CONMAN_KILL':
            # Raise an exception:
            raise ConmanKillSig('A kill signal was received')
        else:
            raise NotImplementedError(f'Cannot interpret command "{command}"')

    # <HANDSHAKE_CODE>
    def build_handshake(self):
        """Constructs & returns the handshake dictionary.

        Returns
        -------
        handshake_dictionary : `dict` [`str`, `int`]
            A dictionary containing all relevant version and buffer info.
        """

        # Perform a handshake to identify which protocol versions should be used
        # Compile the handshake data
        handshake_data = {
            # Current conman protocol version
            'CONMAN': self.PROTO['CONMAN'],
            # Highest available pickle protocol version
            'PICKLE': pickle.HIGHEST_PROTOCOL,
            # Reception buffer size
            'BUFSZ': self._RCVBUF
        }

        # Return the handshake data
        return handshake_data

    def resolve_handshake(self, handshake):
        """Takes the incoming handshake & resolves it to the highest mutual
        version numbers, which are then set for the class instance.

        Parameters
        ----------
        handshake: `dict` [`str`, `int`]
            Incoming handshake dictionary.
        """
        # Get the local version info
        local = self.build_handshake()

        # Loop over the various protocols
        for key in self.PROTO.keys():
            # And identify the highest mutually inclusive protocol version
            self.PROTO[key] = min(handshake[key], local[key])

        # Record target's buffer size: used to identify know how much data can
        # be sent before the target's port blocks.
        self._SNDBUF = handshake['BUFSZ']

    def perform_handshake(self):
        """Performs a handshake operation with the connected entity.
        """
        # Start by constructing and and sending the handshake message.
        self.send_message(self.build_handshake())
        # Wait for the incoming handshake message, and resolve it
        self.resolve_handshake(self.await_message())
    # </HANDSHAKE_CODE>

    # <CONNECTION_CODE>
    def accept_connection(self):
        """Wait for an incoming connection and return a new conman representing
        the connection. This acts as an interface for the socket.accept function.

        Returns
        -------
        connection : `conman`
            new conman socket representing the connection formed.

        Notes
        -----
        This function will block until a connection is established.
         |
        The socket binding and listening operations are carried out here in an
        effort to hide as much of the boilerplate code as possible. This will
        will induce a small overhead but it is negligible.
        """
        # Bind the socket and open it to new connections if not done so already.
        # Unbound sockets will be on port 0.
        if self.getsockname()[1] == 0:
            # Bind the socket to the specified host and port
            self.bind(self.address)
            # Listen for connections with a backlog queue of up to 1000 connections
            self.listen(1000)
            # Set the _is_server status
            self._is_server = True

        # Accept an incoming connection (wait or one if necessary)
        soc, address = self.accept()

        # Convert socket.socket to a conman instance. As the address family and
        # connection type are statically defined in conman only socket protocol
        # and file-number need to be passed.
        conman_soc = self.__class__(address, proto=soc.proto, fileno=dup(soc.fileno()))

        # Perform the handshake operation to identify protocol versions.
        conman_soc.perform_handshake()

        # Finally return the conman
        return conman_soc

    def make_connection(self, t=None):
        """Connect to the remote socket address. This calls socket.connect to
        establish a connection & then performs a handshake.

        Parameters
        ----------
        t : `float`, `int`, optional
            Time in seconds to keep attempting to establish the connection for.
            If None, then only a single attempt will be made which will raise
            an error on failure. [DEFAULT=None]
        """
        # Set the server status
        self._is_server = False
        # If only making a single attempt
        if t is None:
            # Attempt to establish a connection to the target address
            self.connect(self.address)
        # If told to retry
        else:
            # Mark the time of the first connection attempt
            t_init = time()
            # Try to connect until success
            while self.connect_ex(self.address) != 0:
                # or until the time limit 't' is reached
                if time() - t_init > t:
                    # Make one last attempt before raising an error
                    self.connect(self.address)
                # Wait for 1 second before retrying
                sleep(1)

        # Perform the handshake operation to identify protocol version
        self.perform_handshake()

    @property
    def alive(self):
        """Returns True if the connection is still active.

        Returns
        -------
        status : `bool`
            Boolean indicating the presence of an active connection.

        """

        # This check follows standard TCP protocol rules

        # Check if there is any data to be read from the socket
        if self.poll():
            # If there is readable data, but read attempts return nothing
            if len(self.recv(1, MSG_PEEK)) == 0:
                # Then this is a dead socket
                return False
            else:
                # Otherwise this socket is still alive
                return True
        else:
            # If the poll returns no readable data then the socket is still alive
            return True

    def kill(self):
        """Shutdown the socket connection in a graceful manner.
        """
        # Inform connected socket that no further data will be sent.
        self.shutdown(2)
        # Terminate the connection.
        self.close()


class Conjour(Conman):
    """Identical in operation to ``Conman``, but with additional connection
    journaling and record-keeping functions. These features are of particular
    use if a slave is lost before it can submit it's results; in such an
    eventuality the "lost" job can be recovered from a page file and sent to
    another slave. Logging also helps to determine how much more information
    can be sent before the send operation becomes blocking.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.idle = True
        self.data_log = []
        self.journal = (tempfile.TemporaryFile(buffering=0), [])

    @property
    def free_space(self):
        """Calculate the amount of free space in the target's port buffer.

        Returns
        -------
        free_space : ``int``
            The amount of free space, in bytes, available in the target's port
            buffer.

        Notes
        -----
        Note that a 5 % safety margin has been here given the significant
        consequences that can arise from overestimating the amount of free
        space.
         |
        If this this is the server/master side we can omit the first
        message in the send_log as the slave will always consume the
        first message due to its reactive nature. Such a check is not
        strictly speaking necessary as ``Conjour`` instances are only
        used on the server/master side.
        """
        # Calculate the free space, excluding the first message if this is the
        # server/master side of the connection.
        n = 1 - (not self._is_server)  # <-- 0 if server/master, 1 if client/slave
        return max(int(self._SNDBUF * 0.95) - sum(self.data_log[n:]), 0)

    def send_message(self, message, **kwargs):
        """Packs up and sends a message to the connected socket.

        Parameters
        ----------
        message : `serialisable`
             Data to be sent
        **kwargs
            ``pkld``:
                Use to indicate that a message has already been pickled, & forces
                the Pickled flag to True while treating it as a bytes object.
            ``command``:
                Flag used to send command and control messages. [DEFAULT=False]
            ``compress``:
                Compress message prior to sending. [DEFAULT=True]
            ``packed``:
                Flag used to indicate that a message has already been packed.
                [DEFAULT=False]

        Notes
        -----
        Function will continue to block until the entire message has been sent.
        Blocking is encountered when the message size exceeds the available free
        space in the target's port buffer and said buffer is not cleared quickly.
        """

        # Parse the message into a byte-stream and package it into a data-frame,
        # but only if it is not already packed.
        if not kwargs.get('packed', False):
            message = self.pack(message, **kwargs)

        # Send the message
        self.send(message)

        # Don't log command messages as they are small compared to the safety net
        # added to the buffer's size.
        if not kwargs.get('command', False):
            # Set idle status to False
            self.idle = False
            # Append the buffer size that this message would take up to the send_log
            self.data_log.append(CMSG_SPACE(len(message)))
            # Add the message to the page file
            save_to_page([message], *self.journal)

    def await_message(self, **kwargs):
        """Waits until a message is received, unpacks it & returns its content.

        Returns
        -------
        message : `serialisable`, `str`, `bytes`
            Data received from the connected socket.
        **kwargs
            ``timeout``:
                Flag used to specify the time after which the await should be
                aborted and an exception raised. Very small values may cause
                unpredictable results.

        Notes
        ----
        This function will block until a complete message is received.
        """
        # If the timeout is specified then set a timeout value for the duration
        # of this function.
        if 'timeout' in kwargs:
            self.settimeout(kwargs['timeout'])

        message = self._read_message()

        # As the job associated with this message has been run we know that the
        # outbound message must have been removed from the port buffer. Thus
        # we can remove it from the send_log
        del self.data_log[0]
        # Remove the job from the journal and rewrite the page file. The page
        # file must be rewriten otherwise the file size will grow linearly with
        # the number of jobs ran.
        save_to_page(load_from_page(*self.journal)[1:], *self.journal)

        # If the send_log is empty, set status to idle
        if len(self.journal[1]) == 0:
            self.idle = True

        # Reset blocking status
        if 'timeout' in kwargs:
            self.setblocking(True)

        # Return the message
        return message

    def kill(self):
        """Shutdown the socket connection in a graceful manner.
        """
        # Close the journal
        self.journal[0].close()
        # Inform connected socket that no further data will be sent.
        self.shutdown(2)
        # Terminate the connection.
        self.close()
