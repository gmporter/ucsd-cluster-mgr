#!/usr/bin/env python

import sys, argparse, os
sys.path.append('gen-py')

import redis
from redis.exceptions import ConnectionError

from ucsd import ClusterManager
from ucsd.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class ClusterManagerHandler:
  def __init__(self, _debugmode, servername):
    self.debugmode = _debugmode
    self.redis_server = servername

    self.r_server = redis.Redis(self.redis_server)

    try:
      # Redis doesn't actually detect failures until a command is issued,
      # and so we issue a dummy command here to fail fast if the
      # server isn't there
      self.r_server.ping()
    except ConnectionError as ce:
      print "Error connecting to redis server: " + str(ce)
      print "Are you running a redis server on host " + self.redis_server + "?"
      sys.exit(1)

  def debug(self, str):
    if self.debugmode:
      print str

  def login(self, auth_request):
    raise AuthenticationException("login not yet supported")

  def ping(self):
    self.debug('ping()')
    return True

  def host_add(self, hostname, macaddr):
    self.debug("host_add()")

    key = "host_%s" % hostname
    if self.r_server.exists(key):
      # the host already exists
      self.debug("  already exists, doing nothing")
      return False
    
    self.r_server.hset(key, "name", hostname)
    self.r_server.hset(key, "status", HostStatus.AVAILABLE)
    self.r_server.hset(key, "owner", None)
    self.r_server.hset(key, "assigned_project", None)
    self.r_server.hset(key, "netboot_enabled", True)
    self.r_server.hset(key, "macaddr", macaddr)
    self.r_server.hset(key, "tags", None)

    self.debug("  added host %s with mac %s" % (hostname, macaddr))
    return True

  def host_remove(self, hostname):
    self.debug("host_remove()")

    key = "host_%s" % hostname
    if not self.r_server.exists(key):
      self.debug("  host didn't exist, doing nothing")
      return False

    self.r_server.delete(key)
    self.debug("  removed host %s" % hostname)
    return True

  def project_add(self, name, server, rootpath, kernel, initrd, params):
    self.debug("project_add")

    key = "project_%s" % name
    if self.r_server.exists(key):
      # the project already exists
      self.debug("  already exists, doing nothing")
      return False
    
    self.r_server.hset(key, "name", name)
    self.r_server.hset(key, "nfsserver", server)
    self.r_server.hset(key, "nfsroot", rootpath)
    self.r_server.hset(key, "kernel", kernel)
    self.r_server.hset(key, "initrd", initrd)
    self.r_server.hset(key, "params", params)

    self.debug("  added project %s" % name)
    return True

  def project_remove(self, projectname):
    self.debug("project_remove")

    key = "project_%s" % projectname
    if not self.r_server.exists(key):
      self.debug("  project didn't exist, doing nothing")
      return False

    self.r_server.delete(key)
    self.debug("  removed project %s" % projectname)
    return True

  def user_add(self, username, fullname):
    self.debug("user_add")

    key = "user_%s" % username
    if self.r_server.exists(key):
      # the user already exists
      self.debug("  already exists, doing nothing")
      return False
    
    self.r_server.hset(key, "name", username)
    self.r_server.hset(key, "fullname", fullname)

    self.debug("  added user %s" % username)
    return True

  def user_remove(self, username):
    self.debug("user_remove")

    key = "user_%s" % username
    if not self.r_server.exists(key):
      self.debug("  user didn't exist, doing nothing")
      return False

    self.r_server.delete(key)
    self.debug("  removed user %s" % username)
    return True

  def get_projects(self):
    self.debug("get_projects")

    prefix = "project_"
    return [name[len(prefix):] for name in self.r_server.keys(prefix+"*")]

  def __materialize_hosts(self, host_keys):
    hosts = []

    for hkey in host_keys:
      host = Host()

      host.name = self.r_server.hget(hkey, "name")
      status = int(self.r_server.hget(hkey, "status"))
      if status == 0:
        host.status = HostStatus.UNKNOWN
      elif status == 1:
        host.status = HostStatus.ASSIGNED
      elif status == 2:
        host.status = HostStatus.AVAILABLE
      else:
        print('bad status value %d in host %s' % (status, hkey))

      host.owner = self.r_server.hget(hkey, "owner")
      host.assigned_project = self.r_server.hget(hkey, "assigned_project")
      host.netboot_enabled = self.r_server.hget(hkey, "netboot_enabled")
      host.macaddr = self.r_server.hget(hkey, "macaddr")
      host.tags = self.r_server.hget(hkey, "tags")

      hosts.append(host)

    return hosts

  def get_hosts(self, project, tag):
    self.debug("get_hosts")

    # all hosts
    hkeys = self.r_server.keys("host_*")

    # if project is specified, filter all but hosts in that project
    if project is not None and project is not "":
      hkeys = [host for host in hkeys if
        self.r_server.hget(host,"assigned_project") == project]

    # if tag is specified, filter all but hosts with that tag
    if tag is not None and tag is not "":
      hkeys = [host for host in hkeys if
        tag in self.r_server.hget(host,"tags").split(",")]

    return self.__materialize_hosts(hkeys)

  def get_tags(self, host):
    self.debug("get_tags")
    
    key = "host_%s" % host
    if not self.r_server.exists(key):
      return []
    else:
      return self.r_server.hget(key, "tags").split(",")
  
  def host_assign(self, host, project, user):
    self.debug("host_assign")

    key = "host_%s" % host
    if not self.r_server.exists(key):
      self.debug("  host %s didn't exist" % host)
      return False

    # we should probably sanity check the project and user they gave us
    self.r_server.hset(key, "assigned_project", project)
    self.r_server.hset(key, "tags", None)
    self.r_server.hset(key, "owner", user)
    self.r_server.hset(key, "status", HostStatus.ASSIGNED)

    return True

  def host_release(self, host):
    self.debug("host_release")

    key = "host_%s" % host
    if not self.r_server.exists(key):
      self.debug(" host %s didn't exist" % host)

    self.r_server.hset(key, "assigned_project", None)
    self.r_server.hset(key, "tags", None)
    self.r_server.hset(key, "owner", None)
    self.r_server.hset(key, "status", HostStatus.AVAILABLE)

    return True

  def tag_add(self, host, tag):
    self.debug("tag_add")

    key = "host_%s" % host
    if not self.r_server.exists(key):
      self.debug(" host %s didn't exist" % host)

    old_tags = self.r_server.hget(key, "tags")
    self.r_server.hset(key, "tags", old_tags + "," + tag)

    return True

  def tag_removeAll(self, host):
    self.debug("tag_removeAll")

    key = "host_%s" % host
    if not self.r_server.exists(key):
      self.debug(" host %s didn't exist" % host)

    self.r_server.hset(key, "tags", None)

  def lookup(self, macaddr):
    self.debug("lookup")

    # find a host with this macaddr
    h = None
    for hkey in self.r_server.keys('host_*'):
      mac = self.r_server.hget(hkey, 'macaddr')
      if mac.lower() == macaddr.lower():
        host = hkey[5:]
        self.debug('found a match for host %s' % host)

        # is the host assigned to a project?
        status = int(self.r_server.hget(hkey, 'status'))
        if status != HostStatus.ASSIGNED:
          self.debug('host %s was not in assigned mode' % host)
          break

        # lookup what project this host is assigned to
        proj = self.r_server.hget(hkey, 'assigned_project')
        projkey = 'project_' + proj
        self.debug('host %s assigned to project %s' % (host,proj))

        # is the project valid?
        if not self.r_server.exists('project_' + proj):
          self.debug('specified project %s is invalid' % proj)
          break

        # construct the bootconfig and return to the client
        bc = BootConfig()

        bc.project = proj
        bc.kernel = self.r_server.hget(projkey, 'kernel')
        bc.initrd = self.r_server.hget(projkey, 'initrd')
        bc.nfsserver = self.r_server.hget(projkey, 'nfsserver')
        bc.nfsroot = self.r_server.hget(projkey, 'nfsroot')
        bc.parameters = self.r_server.hget(projkey, 'parameters')

        self.debug("found bootconfig record: %s" % str(bc))

        return bc

    # didn't find a match for 'macaddr'
    return None

def start_managerd(debugmode, redis_server):
  print "Starting managerd daemon..."

  print "connecting to redis server " + redis_server

  handler = ClusterManagerHandler(debugmode, redis_server)
  processor = ClusterManager.Processor(handler)
  transport = TSocket.TServerSocket(port=9090)
  tfactory = TTransport.TBufferedTransportFactory()
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()

  server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
  try:
    server.serve()
  except KeyboardInterrupt:
    pass
  finally:
    transport.close()
  print "done"

def main():
  parser = argparse.ArgumentParser(description="Cluster manager daemon")

  parser.add_argument("-d", "--debug", action='store_true',
                      help="debug mode")
  parser.add_argument("--redis_server",
                      help="redis server hostname", default="localhost")
  args = parser.parse_args()

  start_managerd(args.debug, args.redis_server)

if __name__ == "__main__":
  sys.exit(main())
