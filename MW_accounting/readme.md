This is run (and outputs) in the same directory as contains the "accounting" file.

Main bottleneck is memory. This is just the sort of thing that one needs a handy supercomputer for.

My job submission script copies the current accounting file (to cwd) & works on that. 

GOTCHAS: Default histogram size is huge and there's log scales in odd places because otherwise 
you don't see some of the data. Downside: default font size on histos is *tiny*
