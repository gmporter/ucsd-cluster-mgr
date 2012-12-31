#!/usr/bin/env python

# @author John McCulough
# @author George Porter

import warnings
warnings.filterwarnings('ignore','.*',UserWarning,'zope')

import sys
sys.path.append('../../managerd/gen-py')

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
from ucsd import ClusterManager
from ucsd.ttypes import *

from string import Template

from datetime import datetime, timedelta

from twisted.internet.protocol import DatagramProtocol, ServerFactory
from twisted.internet import reactor, task, defer
import logging, logging.handlers
import struct, re, daemon, argparse, os

(OP_RRQ, OP_WRQ, OP_DATA, OP_ACK, OP_ERROR, OP_OACK) = range(1,7)
(ERR_UNDEF, ERR_NOTFOUND, ERR_ACCESS, ERR_DISKFULL, ERR_ILLEGAL,
        ERR_UNKNOWN_TID, ERR_EXISTS, ERR_USER) = range(0,8)
(S_RRQ, S_ACK) = range(0,2)

mac_str = "-".join(['[0-9a-fA-F]{2}' for nil in range(0,6)])
pxe_mac_re = re.compile('/pxelinux.cfg/01-(' + mac_str + ')$')
tftp_path = '/tftproot'

TFTP_PORT = 69
RETRY_TIMEOUT = 5
SESSION_TIMEOUT = 30

logger = logging.getLogger('')

def connect_to_managerd(host, port):
  socket = TSocket.TSocket(host, port)
  transport = TTransport.TBufferedTransport(socket)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ClusterManager.Client(protocol)

  try:
    transport.open()
  except TTransport.TTransportException as te:
    print "Error connecting to cluster manager daemon"
    print str(te)
    sys.exit(1)

  return (transport,client)

def close_managerd(transport):
  if transport:
    transport.close()

def lookup_file(fname):
    global verbose
    global client

    if verbose:
        logger.info("Received request from %s" % fname)

    try:
        pxe_match = pxe_mac_re.match(fname)
        if pxe_match:
            inmac = pxe_match.group(1)
            outmac = ":".join(inmac.split("-"))

            logger.info('looking up mac address %s' % outmac)
            bootconfig = client.lookup(outmac)
            logger.info('got host record: %s' % str(bootconfig))

            kernel = 'nfsroot/vmlinuz-%s' % (bootconfig.kernel,)
            if bootconfig.initrd != '':
                initrd = 'initrd=nfsroot/initrd.img-%s' % (bootconfig.initrd,)
            else:
                initrd = 'noinitrd'
            root = '%s:%s' % (bootconfig.nfsserver,bootconfig.nfsroot,)

            parameters = bootconfig.parameters

            pxeconfig = {
                          'project': bootconfig.project,
                          'kernel': kernel,
                          'root': root,
                          'initrd': initrd,
                          'parameters': parameters,
                        }

            template_file = os.path.join(os.path.dirname(__file__),
                                         'pxelinux.conf')
            template = Template(open(template_file,'r').read())
            cfg = template.safe_substitute(pxeconfig)

            logger.info('Serving project %s to %s' % (bootconfig.project, outmac))

            logger.debug(cfg)

            return cfg
    except IndexError:
        logger.debug('Error looking up host for mac in %s' % fname)
        pass
    except Exception, e:
        logger.debug('Error looking up mac in %s (%s)' % (fname, str(e)))
        return None

    # This is to work around a bug(?) in the OpenSolaris booter
    if fname == "//pxegrub.0":
    	fname = "/pxegrub.0"

    try:
        if os.path.isabs(fname):
            rel_path = fname[1:]
        else:
            rel_path = fname

	full_path = os.path.realpath(os.path.join(tftp_path, rel_path))
	common_prefix = os.path.commonprefix([full_path, tftp_path])

        # Disallow escaping from the tftp root
        if not common_prefix.startswith(tftp_path):
            logger.error('refusing to serve %s' % (fname,))
            return None
        return open(full_path,'r').read()
    except Exception, e:
        logger.debug('Error opening file %s (%s)' % (fname, str(e)))
        pass
    return None

class TFTPSession(object):
    def __init__(self, address):
        self.address = address
        self.state = S_RRQ
        self.data_block = 0
        self.block_size = 512
        self.timeout = 5
        self.timeout_event = defer.Deferred()
        self.timeout_event.addCallback(self.clearTimers)

    def clearTimers(self, *args):
        try:
            self.retry_timer.cancel()
        except:
            pass

    def handle_datagram(self, dg, send_func):
        (opcode,) = struct.unpack('!H', dg[0:2])

        self.last_time = datetime.now()

        if self.state == S_RRQ:
            if opcode != OP_RRQ:
                return False
            return self.handle_rrq(dg[2:], send_func)
        elif self.state == S_ACK:
            if opcode != OP_ACK:
                return False
            return self.handle_ack(dg[2:], send_func)
        else:
            print "Unhandled opcode", opcode
            return False

    def handle_rrq(self, dg, send_func):
        args = (dg.split('\0'))[:-1]

        fname_str = args[0]
        mode_str = args[1]

        opt_count = (len(args) - 2) / 2

        ack_args = []

        self.data = lookup_file(fname_str)
        if self.data is None:
            self.send_error(ERR_NOTFOUND, fname_str + " not found", send_func)
            return False

        for i in range(1,opt_count+1):
            option = args[2*i]
            value = args[2*i+1]

            if option == "blksize":
                self.block_size = int(value)
                ack_args.extend(["blksize", value])
            elif option == "tsize":
                ack_args.extend(["tsize", str(len(self.data))])

        #print args

        self.state = S_ACK

        if len(ack_args):
            self.data_block = -1
            return self.send_oack(ack_args, send_func)
        else:
            self.data_block = 0
            return self.send_block(1, send_func)

    def handle_ack(self, dg, send_func):
        (block_num,) = struct.unpack('!H', dg)

        if self.data_block == -1 and block_num == 0:
            self.data_block = 0
            return self.send_block(1, send_func)

        delta = (self.data_block & 0xffff) - block_num

        return self.send_block(delta + 1, send_func)

    def send_error(self, err, msg, send_func):
        send_func(struct.pack('!HH', OP_ERROR, err) + msg + '\0')
        return False

    def send_oack(self, args, send_func):
        send_func(struct.pack('!H', OP_OACK) + "\0".join(args) + "\0")
        return True

    def send_block(self, delta, send_func):
        try:
            self.retry_timer.cancel()
        except:
            pass

        if delta != 0:
            try:
                self.timeout_timer.cancel()
            except:
                pass

        self.data_block = block_num = self.data_block + delta

        low_index = (block_num - 1) * self.block_size
        high_index = (block_num) * self.block_size

        if low_index > len(self.data):
            return False

        block = self.data[low_index:high_index]

        send_func(struct.pack('!HH', OP_DATA, self.data_block & 0xffff) + block)

        self.retry_timer = reactor.callLater(1, self.send_block, 0, send_func)

        # Don't reset the timer on a retransmit
        if delta != 0:
            self.timeout_timer = reactor.callLater(
                                               self.timeout,
                                               self.timeout_event.callback,
                                               self)

        return True

    def timed_out(self, now):
        return (now - self.last_time) > self.timeout

class TFTP(DatagramProtocol):
    def __init__(self):
        self.sessions = {}

        # self.timer = task.LoopingCall(self.testTimeout)
        # self.timer.start(1)

    def stopProtocol(self):
        self.sessions = {}

    def startProtocol(self):
        self.sessions = {}

    def testTimeout(self):
        now = datetime.now()

        for addr in self.sessions.keys():
            session = self.sessions[addr]
            if session.timed_out(now):
                del self.sessions[addr]

    def removeSession(self, session):
        try:
            session.clearTimers()
            del self.sessions[session.address]
        except:
            pass

    def datagramReceived(self, datagram, address):
        if self.sessions.has_key(address):
            session = self.sessions[address]
        else:
            self.sessions[address] = session = TFTPSession(address)
            session.timeout_event.addCallback(self.removeSession)

        res = session.handle_datagram(datagram,
                                  lambda d: self.transport.write(d, address))
        if not res:
            self.removeSession(session)

def run_reactor():
    logger.info('SEED TFTP Starting')
    reactor.listenUDP(TFTP_PORT, TFTP())
    reactor.run()

def main():
    global verbose
    global client
    global tftp_path

    parser = argparse.ArgumentParser(description="Cluster manager tftp server",
                       formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--server", help="managerd server",
                        default="localhost")
    parser.add_argument("-p", "--port", help="managerd port",
                        default=9090, type=int)
    parser.add_argument("-f", "--foreground", help="Foreground mode",
                        action="store_true")
    parser.add_argument("-v", "--verbose", help="Verbose",
                        action="store_true")
    parser.add_argument("-d", "--pidfile",
                        help="Pidfile location",
                        default="/var/run/seed-tftp.pid")
    parser.add_argument("-t", "--test",
                        help="Testmode (manually look up mac addr)",
                        nargs=1)
    parser.add_argument("-r", "--rootpath",
                        help="tftp root path", default="/tftproot")

    args = parser.parse_args()
    verbose = args.verbose

    logger.setLevel(logging.DEBUG)

    if args.foreground:
        logger.addHandler(logging.StreamHandler(sys.stderr))
    else:
        logger.addHandler(logging.handlers.SysLogHandler("/dev/log"))

    tftp_path = args.rootpath

    (transport, client) = connect_to_managerd(args.server, args.port)

    if args.test:
      lookup_file('/pxelinux.cfg/01-' + args.test[0])
      close_managerd(transport)
      sys.exit(0)

    try:
        d = None
        if not args.foreground:
            sys.stdin.close()
            class NullDevice:
                def write(self, s):
                    pass

            sys.stdout = NullDevice()
            sys.stderr = NullDevice()
            pid = os.fork()
            if pid:
                print >>open(args.pidfile, 'w'), pid
                sys.exit(0)

            os.setsid()

            # This package apparently sucks
            #d = daemon.DaemonContext(pidfile=args.pidfile)
            #print d.open()

        run_reactor()
    except Exception, e:
        logger.error(str(e))
    finally:
        close_managerd(transport)

if __name__ == '__main__':
    main()
