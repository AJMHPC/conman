Static Farming
==============
This is a minimal working example of a master and two slaves operating in "*send
and await*" mode. In this mode; all jobs are sent out at once and then the master
blocks until all submitted jobs have returned a result. For this example each
job is a tuple of numbers, which are multiplied and sent back along with their
product. This example can be run by the following steps:
1. Ensure the `host` and `port` variables in the server/worker are identical.
2. Start up a single server instance by calling `python server.py`.
3. Start up a number of worker instances equal to the `await_n` value set in
the _master.py_ file (default=2).

Each slave will print out the jobs that they are working on and the master will
print out the results it receives.


#####Troubleshooting
If the error `OSError: [Errno 98] Address already in use` is encountered then
the selected `port` is already in use. This can happen if the script is run
twice in quick succession or some other program is using the port. To fix this
simply change the `port` to a different value.

#####General Notes
This code only facilitates the sending of receiving of data, it is up to the user
to decide how to interpret was is sent and what is an appropriate response is.
