#!/bin/bash

CMD="../src/clients/py/clustermgr-cli.py"

kern_params="console=ttyS1,115200 console=tty0 syslog=172.22.16.94"

# Load a few hosts
${CMD} host_add dcswitch61 00:11:22:33:44:61
${CMD} host_add dcswitch62 00:11:22:33:44:62
${CMD} host_add dcswitch63 00:11:22:33:44:63
${CMD} host_add dcswitch64 00:11:22:33:44:64
${CMD} host_add dcswitch65 00:11:22:33:44:65

# Create a few projects
${CMD} project_add --params "console=ttyS1,115200 console=tty0 syslog=172.22.16.94" proj1 nfs.ucsd.edu /mnt/root/proj1 2.6.32 2.6.32
${CMD} project_add --params "console=ttyS1,115200 console=tty0 syslog=172.22.16.94" proj2 nfs.ucsd.edu /mnt/root/proj2 2.6.32 ""
${CMD} project_add --params "console=ttyS1,115200 console=tty0 syslog=172.22.16.94" proj3 nfs.ucsd.edu /mnt/root/proj3 2.6.35 2.6.35

# a few users
${CMD} user_add alice "Alice User"
${CMD} user_add bob "Bob User"
${CMD} user_add oscar "Oscar User"

# Assign some hosts
${CMD} host_assign --user alice dcswitch61 proj1
${CMD} host_assign --user alice dcswitch62 proj1
${CMD} host_assign --user bob dcswitch63 proj2
${CMD} host_assign --user bob dcswitch64 proj3

# and some tags
${CMD} tag_add dcswitch61 client
${CMD} tag_add dcswitch62 server
${CMD} tag_add dcswitch63 t1
${CMD} tag_add dcswitch63 t2
${CMD} tag_add dcswitch63 t3
