#!/usr/bin/env python3

import sys, unittest, argparse, getpass, os
from urlparse import urlparse
dir = os.path.dirname(__file__)
sys.path.append(os.path.join(dir, '../../managerd/gen-py'))

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

from ucsd import ClusterManager
from ucsd.ttypes import *


#
# the API
#

def ping(client, args):
  client.ping()

def host_add(client, args):
  return client.host_add(args.hostname, args.macaddr)

def host_remove(client, args):
  return client.host_remove(args.hostname)

def proj_add(client, args):
  return client.project_add(args.name, args.rootserver, args.rootpath,
                            args.kernel, args.initrd, args.params)

def proj_remove(client, args):
  return client.project_remove(args.name)

def user_add(client, args):
  return client.user_add(args.username, args.fullname)

def user_remove(client, args):
  return client.user_remove(args.user)

def get_projects(client, args):
  for project in client.get_projects():
    print(project)

def get_hosts(client, args):
  pairs = args.projspec.split(":")

  project = pairs[0]
  tag = pairs[1] if len(pairs) > 1 else ''

  hosts = client.get_hosts(project, tag)

  for host in hosts:
    print(host.name)

def get_tags(client, args):
  tags = client.get_tags(args.host)

  for tag in tags:
    print(tag)

def mapping(client, args):
  hosts = client.get_hosts(None, None)

  for host in hosts:
    print('%s: <%s> %s' % (host.name, host.assigned_project, host.tags))

def host_assign(client, args):
  client.host_assign(args.host, args.project, args.user)

def host_release(client, args):
  client.host_release(args.host)

def tag_add(client, args):
  client.tag_add(args.host, args.tag)

def tag_removeAll(client, args):
  client.tag_removeAll(args.host)

#
# connection handling
#

def connect_to_managerd(host, port):
  socket = TSocket.TSocket(host, port)
  transport = TTransport.TBufferedTransport(socket)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ClusterManager.Client(protocol)

  try:
    transport.open()
  except TTransport.TTransportException as te:
    print("Error connecting to cluster manager daemon")
    print(str(te))
    sys.exit(1)

  return (transport,client)

def close_managerd(transport):
  if transport:
    transport.close()

#
# argument handling and main()
#

def main():
  parser = argparse.ArgumentParser(description="Cluster manager client",
                formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument("-s", "--server", help="manager servername",
                      default="localhost")
  parser.add_argument("-p", "--port", help="manager port",
                      type=int, default=9090)
  subparsers = parser.add_subparsers(help='sub-command help')

  # ping
  parser_ping = subparsers.add_parser('ping', help='ping the managerd server')
  parser_ping.set_defaults(func=ping)

  # host_add
  parser_hostadd = subparsers.add_parser('host_add', help='add a new host')
  parser_hostadd.add_argument('hostname', help='name of the host to add')
  parser_hostadd.add_argument('macaddr', help='macaddr of host')
  parser_hostadd.set_defaults(func=host_add)

  # host_remove
  parser_hostrem = subparsers.add_parser('host_remove', help='remove a host')
  parser_hostrem.add_argument('hostname', help='name of the host to remove')
  parser_hostrem.set_defaults(func=host_remove)

  # project_add
  parser_projadd = subparsers.add_parser('project_add',
                                         help='add a new project')
  parser_projadd.add_argument('name', help='the project name')
  parser_projadd.add_argument('rootserver', help='the nfs server')
  parser_projadd.add_argument('rootpath', help='the path to the root')
  parser_projadd.add_argument('kernel', help='the kernel version')
  parser_projadd.add_argument('initrd', help='the initrd version')
  parser_projadd.add_argument('--params',
                              help='kernel cmdline parameters',
                              default="")
  parser_projadd.set_defaults(func=proj_add)

  # project_remove
  parser_projrem = subparsers.add_parser('project_remove',
                                         help='remove a project')
  parser_projrem.add_argument('name', help='name of the project to remove')
  parser_projrem.set_defaults(func=proj_remove)

  # user_add
  parser_useradd = subparsers.add_parser('user_add', help='add a new user')
  parser_useradd.add_argument('username', help='username to add')
  parser_useradd.add_argument('fullname', help='full name of the user')
  parser_useradd.set_defaults(func=user_add)

  # user_remove
  parser_userrem = subparsers.add_parser('user_remove', help='remove a user')
  parser_userrem.add_argument('user', help='user to remove')
  parser_userrem.set_defaults(func=user_remove)

  # get_projects
  parser_getproj = subparsers.add_parser('get_projects',
                                         help='query projects')
  parser_getproj.set_defaults(func=get_projects)

  # get_hosts
  parser_gethosts = subparsers.add_parser('get_hosts',
                                          help='query hosts')
  parser_gethosts.add_argument('projspec', help='project[:tag] specification',
                               default="", nargs='?')
  parser_gethosts.set_defaults(func=get_hosts)

  # get_tags
  parser_gettags = subparsers.add_parser('get_tags', help='query tags')
  parser_gettags.add_argument('host', help='host to query')
  parser_gettags.set_defaults(func=get_tags)
  
  # mapping
  parser_mapping = subparsers.add_parser('mapping', help='host mapping')
  parser_mapping.set_defaults(func=mapping)

  # host_assign
  parser_hostassign = subparsers.add_parser('host_assign',
                                            help='assign a host to a project')
  parser_hostassign.add_argument('host', help='host to assign')
  parser_hostassign.add_argument('project', help='project to assign host to')
  parser_hostassign.add_argument('--user', help='user who should own host',
                                 default=getpass.getuser())
  parser_hostassign.set_defaults(func=host_assign)

  # host_release
  parser_hostrelease = \
    subparsers.add_parser('host_release', help='unassign a host from a project')
  parser_hostrelease.add_argument('host', help='host to release')
  parser_hostrelease.set_defaults(func=host_release)

  # tag_add
  parser_tagadd = subparsers.add_parser('tag_add', help='add a new tag')
  parser_tagadd.add_argument('host', help='host to tag')
  parser_tagadd.add_argument('tag', help='tag')
  parser_tagadd.set_defaults(func=tag_add)

  # tag_removeAll
  parser_tagRemAll = subparsers.add_parser('tag_removeAll',
                                             help='remove tags from a host')
  parser_tagRemAll.add_argument('host', help='host to clear tags from')
  parser_tagRemAll.set_defaults(func=tag_removeAll)

  args = parser.parse_args()

  (transport,client) = connect_to_managerd(args.server, args.port)
  args.func(client, args)
  close_managerd(transport)

if __name__ == "__main__":
  sys.exit(main())
