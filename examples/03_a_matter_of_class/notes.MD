Notes on Using ConMan With Classes
==================================
This example demonstrates one of the main complications encountered when using
classes with python based task-farms like ConMan. The python global interpreter
lock means no two python instances may have access to the same block of memory/
variables. Thus, farming out a class instance as a job will result in the salve
working on a copy of said class, rather than the original. This results in the
original class instance never getting updated, which is something that the user
may expect and or be relying on. The this example demonstrate this behaviour and
shows a couple of possible solutions.

#####Notes on MPI
While it may technically be possible the GLI using some mpi4py wizardry it could
only be done so for information shared across a single compute node, which is not
all that useful.

