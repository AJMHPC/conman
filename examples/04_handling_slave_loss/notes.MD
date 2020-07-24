Handling Worker Loss
===================
During operation it is possible that one or more workers may become disconnected.
If a worker disconnects only transiently, i.e. it temporary looses its connection
to the internet, then it will be reconnected through the magic of TCP without
issue. However, if a worker crashes due to a critical error then it is considered
to be *lost". When a worker is lost it is striped from the available workers list,
its jobs are reallocated, and the coordinator's `_lost_worker_count` incremented. If
the `_lost_worker_count` surpasses the maximum permitted value, as defined by
`max_worker_loss`, then a `ConmanMaxWorkerLoss` error will be raised. This value
can be specified using the `max_worker_loss` keyword argument during the coordinator's
instantiation. As the loss of multiple workers can indicate a but in the code on
the worker's side is is generally a good idea to keep the `max_worker_loss` value low.

In this example two workers will be mounted and one randomly terminated to
demonstrate the effect that this has.

#####Notes
This code also examples the "poisoned job" effect this occurs when a job is
malformed or its execution in some way results in the destruction of the worker
running it. The job reallocation operation that takes place upon worker loss
will result in this job being passed from one worker to another until all salves
are lost. This is why it is important to handle exceptions on the worker side
studiously and deal with them accordingly.

By default a `ConmanNoWorkersFound` exception will be raised by the coordinator if all
workers have been lost, even when the number of lost workers is technically less
than the `max_worker_loss` parameter. This behaviour can be turned off by setting
the `no_worker_kill` keyword to `False` during coordinator instantiation.






