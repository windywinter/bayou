gl345
Slip days used (this project): 2      Slip days used (total): 4

ml2436
Slip days used (this project): 2      Slip days used (total): 3


Requirements: Linux OS, Python 2.7.12.

We only have "server" and "client" python scripts, not "./build" needed and no ./src/ directory
s
Master facing port defined by master. server facing ports are 20000 + pid and 25000 + pid, where pid is from 0 to n-1.

###################################################
Test cases:
Aside from the list of test cases provided, we added:

1. deperr2.input
Another case that RYW is broken.

2. deperr3.input
A case that MR is broken.

3. deperr4.input
A case that WFR is broken.

4. deperr5.input
A case that MW is broken.

5. functional2.input
Multiple servers retire but none of them is primary.

6. functional3.input
A client put song1 through server 1 and song2 through server 2. Then the client tries to retieve song1 from server 2 and song2 from server 1.

7. nokey.input
A client tries to get a key that does not exist.

8. primaryCommitFirst.input
This case demonstrates that put requests sent to the primary server are commited before others.

9. primaryRetire2.input
The primary server retires and the next primary server retires too.

10. primaryRetire3.input
The primary server, the next-to-be primary server, and the next-next-to-be primary server got retired at the same time (two consective both-sides-retires).

11. recursiveV.input
This case is designed to make a server's version vector lacks 2 slots from another server, to test if recurssive calculation of version vector is implemented correctly.

12. rollback.input
Some committed logs are received after tentative logs are executed, therefore rollback is needed.