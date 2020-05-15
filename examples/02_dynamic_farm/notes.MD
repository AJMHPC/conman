Dynamic Farm
============
This example demonstrates a master operating in "*dynamic*" mode. In this mode,
jobs are sent off as they become available and results are received as they are
completed. This is more flexible than the "*static*" mode and is most use when
not all jobs are available from the very start, i.e. they depend on results from
other calculations, or there are significantly more jobs than slaves. This allows
results to be processed while others are still running. This mode is the primary
way in which ConMan is intended to be used. See *01_static_farm* notes for run
instructions.



#####General Notes
It should be noted that there are technically no "*modes*" per se, and that the
difference between "*static*" and "*dynamic*" is more of a conceptual one. That
is to say there is no variable that controls which "*mode*" is used, its all in
the approach taken by the user. 