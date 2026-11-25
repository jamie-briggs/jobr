Project History
===============

2026-11-24 Started the project by hand, used yoke/deepseek to add the Broker module and tests.


Issues
======

2026-11-24
----------
Yoke puts an entry into the PATH that causes the erlang it uses to be used by the the agent.
The agent figures it out but gives erroneous directions on how to fix the problem.  I added
directions to the beginning of a prompt to keep it from trying that but it is still too noisy
for my taste:

  Fix the path using 'export PATH="/opt/homebrew/opt/erlang/bin:$PATH"' first, then heck that all the tests are passing, please

I used '/permissions yolo' to stop all of the requests to run tools.

I added a rule: 

  rules add <all: Fix environment PATH using 'export PATH="/opt/homebrew/opt/erlang/bin:$PATH"' before running erlang>

That seems really round-about, but it seems to fix the problem.


