Handling Slave Loss
===================
During operation it is possible that one or more slaves may become disconnected.
If a slave disconnects only transiently, i.e. it temporary looses its connection
to the internet, then it will be reconnected through the magic of TCP without
issue. However, if a slave crashes due to a critical error then it is considered
to be *lost". When a slave is lost it is striped from the available slaves list,
its jobs are reallocated, and the master's `_lost_slave_count` incremented. If
the `_lost_slave_count` surpasses the maximum permitted value, as defined by
`max_slave_loss`, then a `ConmanMaxSlaveLoss` error will be raised. This value
can be specified using the `max_slave_loss` keyword argument during the master's
instantiation. As the loss of multiple slaves can indicate a but in the code on
the slave's side is is generally a good idea to keep the `max_slave_loss` value low.

In this example two slaves will be mounted and one randomly terminated to
demonstrate the effect that this has.

#####Notes
This code also examples the "poisoned job" effect this occurs when a job is
malformed or its execution in some way results in the destruction of the slave
running it. The job reallocation operation that takes place upon slave loss
will result in this job being passed from one slave to another until all salves
are lost. This is why it is important to handle exceptions on the slave side
studiously and deal with them accordingly.

By default a `ConmanNoSlavesFound` exception will be raised by the master if all
slaves have been lost, even when the number of lost slaves is technically less
than the `max_slave_loss` parameter. This behaviour can be turned off by setting
the `no_slave_kill` keyword to `False` during master instantiation.






