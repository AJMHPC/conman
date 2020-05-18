Notes on Using ConMan With Classes
==================================
This example demonstrates one of the main complications encountered when using
classes with python based task-farms like ConMan. The python global interpreter
lock means no two python instances may have access to the same block of memory/
variables. Thus, farming out a class instance as a job will result in the salve
working on a copy of said class, rather than the original. This results in the
original class instance never getting updated, which is something that the user
may expect and or be relying on. This example is designed to demonstrate this
behaviour. Possible solutions to this quandary are:
1. Just overwrite the local class instance with the returned one. This, however,
may cause continuity issues with linked objects.
2. Build a transposition function into the class which takes one class as an
argument and effectively clones its attributes. This could be achieved using:
`self.__dict__.update(other.__dict__)`.
3. The previous 2 solutions require a new class instance to be returned. Which
could be too memory intensive for large classes. Thus it may be more reasonable
to either a) return an array/list/dictionary which can be used to update the
class, or b) use external functions to handle to perform the calculations.

#####Warnings
It should be noted that upon receiving a pickled class instance, a slave will
immediately attempt to import the module from which said class came from and
it will attempt to do so in the same way. Thus, this is unable work with any
dynamically defined classes. Efforts must be made to ensure that any classes
that one intends to send over `ConMan` are accessible by both the master and
the slave. 


#####Notes on MPI
While it may technically be possible the GLI using some mpi4py wizardry it could
only be done so for information shared across a single compute node, which is not
all that useful.

