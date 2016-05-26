"""
.. moduleauthor:: Sandeep Nanda <mail: sandeep.nanda@guavus.com> <skype: snanda85>

This module provides the base methods to be used with node handles

Most of the methods are used internally by the framework.
View Source to take a look at all the methods.
"""

import pexpect
import sys
import re
import time
import commands
import os

from potluck.logging import logger
from potluck.nodes import CLI_MODES
from potluck import utils
from settings import REMOTE_DIR, ROOT_DIR

image_fetch_errors = [
    "The requested URL returned error",
    "couldn't connect to host"
]
image_install_errors = [
    "*** Could not",
]


control_chars = ''.join(map(unichr, range(0,10) + range(11, 13) + range(14, 32) + range(127,160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))

def remove_control_chars(s):
    return control_char_re.sub('', s)

#ansi_escape_chars = re.compile(r'(?m)(\x1b[^m]*m\s*$)|(\x1b(.+)\x1b[=>]\s*$)')
ansi_escape_chars = re.compile(r'(?m)((\x1b[^m\r\n]*m)|(\x1b[^\r\n]+\x1b[=>]))')
def remove_special(s):
    """Removes special characters (mostly ANSI) seen in command outputs, from gingko platform onwards"""
    return ansi_escape_chars.sub('', s)

class Node(object):
    """Base class for all type of Nodes. Specific node types (like Insta, Collector etc.)
    are inherited from this class

    Default values associated with all nodes are:

    * Username: admin
    * Connection type: ssh
    * Connection port: 22
    """

    def __init__(self, node):
        """
        constructor to initialize various parameters IP and Port of a node. This can be overridden
        in the derived classes if the nodes use some non-default parameters
        like connection type, port number, username

        `node` should be dict with the below format::

            {
                'alias': 'UI1',
                'type': 'NameNode',
                'connection': 'ssh',
                'ip': '192.168.154.7',
                'port': '22'
                'password': 'admin@123',
            }
        """
        # _session will hold the underlying pexpect session handle
        self._session = None
        self.alias = node["alias"]
        self.ip = node["ip"]
        self.password = node["password"]
        self.port = node["port"]
        self.connection_type = node["connection"]
        self.info = node
        self.promptuser = r"[\r\n][^\r\n]* > "
        self.promptenable = r"[\r\n][^\r\n\(\)]* # "
        self.promptconfig = r"[\r\n][^\r\n]* \(config\) # "
        self.promptshell = r"[\r\n](\x1b\[[^h]+h)?\[admin\@[-\.a-zA-Z0-9]+ [-\w~]+\]\# "
        self.promptpmx = r"[\r\n](\x1b\[[^h]+h)?pm extension[()\w\s]*> "
        self.promptmysql = r"mysql>"
        self.prompts = {
            "user"   : re.compile(self.promptuser),
            CLI_MODES.enable : re.compile(self.promptenable),
            CLI_MODES.config : re.compile(self.promptconfig),
            CLI_MODES.shell  : re.compile(self.promptshell),
            CLI_MODES.pmx    : re.compile(self.promptpmx),
            CLI_MODES.mysql  : re.compile(self.promptmysql)
        }
        self.promptmore = re.compile(r"lines \d+-\d+")
        self._mode = None
        self._mode_stack = []
        self.connect()

    def __str__(self):
        return "%s <%s>" % (self.alias, self.ip)

    def __remote_id__(self):
        """Returns the ID of the object to be used for remoting testcases"""
        return hash(self)

    def __getattr__(self, name):
        try:
            # Provides access to the members of node info dict
            return getattr(self.info, name)
        except AttributeError:
            raise AttributeError("Parameter '%s' is not present in node '%s'" % (name, self.alias))

    def connect(self):
        """Internally used by Potluck to connect to the node, by taking care of the connection type used.

        For now, only ssh connections are supported
        """
        if self.connection_type == "ssh":
            self._session = self.connectSsh()
        else:
            raise NotImplementedError("Connection type not implemented: %s" % connection_type)

    def disconnect(self):
        self._session.close()

    def isConnected(self):
        """Returns True is the node is connected, otherwise returns False"""
        if self._session is None:
            return False
        return self._session.isalive() is True

    def reconnect(self):
        current_mode = self.getMode()
        self.disconnect()
        self.connect()
        # Switch back to the mode that we were already in
        self.setMode(current_mode)

    def connectSsh(self):
        """__
        Connects to the node using ssh and returns the session handle
        """
        connect_handle = pexpect.spawn("ssh -q -o StrictHostKeyChecking=no admin@%s" % self.ip)
        connect_handle.setwinsize(800,800)
        connect_handle.logfile_read = sys.stdout
        #connect_handle.logfile_send = sys.stdout
        i = 0
        ssh_newkey = r'(?i)Are you sure you want to continue connecting'
        remote_key_changed = r"REMOTE HOST IDENTIFICATION HAS CHANGED"

        perm_denied = r"(?i)Permission denied"
        while True:
            i = connect_handle.expect([ssh_newkey, 'Password: ', self.promptuser, self.promptenable,
                                        pexpect.EOF, pexpect.TIMEOUT,
                                        remote_key_changed, perm_denied])
            if i==0:
                connect_handle.sendline('yes')
                continue
            elif i==1:
                 logger.info("Password supplied")
                 connect_handle.sendline(self.password)
                 continue
            elif i==2:
                connect_handle.sendline("enable")
                continue
            elif i==3:
                self._mode = CLI_MODES.enable
                self._prompt = self.promptenable
                break
            elif i==4:
                logger.info("Connection closed: %s" % self)
                logger.info(connect_handle.before) # print out the result
                raise ValueError("Connection Closed: %s" % self)
            elif i==5:
                logger.warning("Timeout while waiting for connection")
                logger.info(connect_handle.before) # print out the result
                raise ValueError("Unable to establish connection %s" % self)
            elif i==6:
                logger.warn("Removing offending key from .known_hosts..")
                known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")

                if "darwin" in sys.platform.lower():
                    # MAC OS
                    utils.run_cmd("sed -i 1 's/%s.*//' %s" % (self.ip, known_hosts_file))
                elif "linux" in sys.platform.lower():
                    # Linux
                    utils.run_cmd("sed -i 's/%s.*//' %s" % (self.ip, known_hosts_file))

                connect_handle = pexpect.spawn("ssh admin@%s" % self.ip)
                connect_handle.setwinsize(800,800)
                connect_handle.logfile_read = sys.stdout

                continue
            elif i==7:
                logger.warning("Permission denied: %s" % self)
                logger.info(connect_handle.before) # print out the result
                raise ValueError("Permission denied: %s." % self)
        return connect_handle

    def resetStream(self, stream=None):
        # Stream should be reset everytime, otherwise cleanup action's log won't be printed in any other file
        if stream is None:
            self._session.logfile_read = sys.stdout
        else:
            self._session.logfile_read = stream

    def initShell(self):
        self.sendCmd("unalias cp ls")

    def sendCmd(self, cmd, timeout=300, ignoreErrors=False):
        """Sends a command to node and returns the output of the command

        :param cmd: Command to send to the node
        :param timeout: Time to wait for the command to complete
        :param ignoreErrors: If ignoreErrors is False, any error in the
                             command output will be logged as logger.error
        """
        self.resetStream()

        cmd = cmd.strip()
        cmd = re.sub(r"[\r\n\t\s]+", " ", cmd)

        try:
            self._session.read_nonblocking(size=1000, timeout=0.5)   # Read all available output
        except pexpect.TIMEOUT: pass

        self._session.sendline(cmd)
        self.last_output = ""
        while True:
            i = self._session.expect([self._prompt, pexpect.EOF, pexpect.TIMEOUT, "logging out", self.promptmore], timeout=timeout)
            if i == 0:
                # Prompt found
                self.last_match = self._session.match
                self.last_output += self._session.before
                break
            if i == 1:
                # EOF
                logger.error("Connection closed %s" % self)
                raise ValueError("Connection Closed")
            elif i == 2:
                # TIMEOUT
                logger.error(str(self._session))
                logger.error("Time Out")
                raise ValueError("Time Out")
            elif i == 3:
                logger.info("Logged out due to inactivity. Reconnecting..")
                self.reconnect()
                self._session.sendline(cmd)
                continue
            elif i == 4:
                # More prompt. Send Space
                self.last_output += self._session.before
                self._session.send(" ")
                continue

        #logger.debug("Output Before Removing command: %s" % self.last_output)
        self.last_output = re.sub("(?m)" + re.escape(cmd), "", self.last_output)
        #logger.debug("Output After Removing command: %s" % self.last_output)

        if not ignoreErrors and re.search("\b(error|unable|failed|failure|unrecognized command):*\b", self.last_output, re.I):
            logger.error("Error while executing command")

        if cmd.startswith("hadoop"):
            #logger.debug("Before removal: '%s'" % self.last_output)
            self.last_output = re.sub(r"(?m)^\s*WARNING:.*$", "", self.last_output)
            #logger.debug("After removal: '%s'" % self.last_output)

        # Remove some special characters seen in new platforms (gingko onwards)
        #logger.debug("Output before removing special chars: %s" % self.last_output)
        ret_val = remove_special(self.last_output)
        #logger.debug("Output after removing special chars: %s" % ret_val)
        return ret_val.strip()

    def sendMultipleCmd(self, cmds, *args, **kwargs):
        consolidated_output = ""
        for cmd in cmds.split("\n"):
            consolidated_output += self.sendCmd(cmd, *args, **kwargs)
            consolidated_output += "\n"
        return consolidated_output

    def getReturnCode(self):
        """Gets the return code of the last executed command on the node"""
        retcode = self.sendCmd("echo $?")
        try:
            return int(retcode)
        except:
            return retcode

    def getModeForPrompt(self, prompt):
        for k, v in self.prompts.iteritems():
            if v == prompt:
                return k

    def getPromptForMode(self, mode):
        return self.prompts[mode]

    def popMode(self):
        if not self._mode_stack:
            logger.warning("Tried to popMode but Mode Stack is empty. Make sure pushMode was called earlier")
            return None
        prev_mode = self._mode_stack.pop()
        return self.setMode(prev_mode)

    def pushMode(self, targetmode):
        self._mode_stack.append(self.getMode())
        return self.setMode(targetmode)

    def guessMode(self, cmd=False):
        """Guess the mode in which the device is currently in.

        :param cmd: True, if you need to send a newline to get the prompt
        """
        self.resetStream()
        if cmd is True:
            self._session.sendline("")

        i = self._session.expect([pexpect.EOF, pexpect.TIMEOUT] + self.prompts.values())
        if i == 0:
            logger.error("Connection closed")
            raise ValueError("Connection Closed")
        elif i == 1:
            logger.error(str(self._session))
            logger.error("Timeout while waiting for prompt")
            logger.warn(self._session.before)
            raise ValueError("Prompt not found")
        else:
            self._prompt = self._session.match.re
            #logger.debug("Prompt matched: %s" % self._prompt.pattern)
            #logger.debug("Output from device: (%s, %s)" % (self._session.before, self._session.after))
            self._mode = self.getModeForPrompt(self._prompt)
        return self._mode
        
    def getMode(self):
        """Returns the current mode of the device"""
        return self._mode

    def setMode(self, targetmode):
        """Sets a mode on the device.

        The following modes are supported:

        #. enable
        #. config
        #. shell
        #. pmx
        #. mysql
        """
        self.resetStream()

        if targetmode not in self.prompts.keys():
            raise ValueError("Invalid Mode %s" % targetmode)

        initialmode = self.getMode()
        if targetmode == initialmode:
            logger.debug("In %s mode" % targetmode)
            return True

        logger.debug("Changing mode from '%s' to '%s' on %s" % (initialmode, targetmode, self))

        # Provide all permutations of mode switching
        if   targetmode == CLI_MODES.config and initialmode == CLI_MODES.enable:
            self._session.sendline("config terminal")
        elif targetmode == CLI_MODES.config and initialmode == CLI_MODES.shell:
            self._session.sendline("cli -m config")
        elif targetmode == CLI_MODES.config and initialmode == CLI_MODES.pmx:
            self._session.sendline("quit")
        elif targetmode == CLI_MODES.enable and initialmode == CLI_MODES.shell:
            self._session.sendline("cli -m enable")
        elif targetmode == CLI_MODES.enable and initialmode == CLI_MODES.config:
            self._session.sendline("exit")
        elif targetmode == CLI_MODES.shell and initialmode == CLI_MODES.enable:
            self._session.sendline("_shell")
        elif targetmode == CLI_MODES.shell and initialmode == CLI_MODES.config:
            self._session.sendline("_shell")
        elif targetmode == CLI_MODES.shell and initialmode == CLI_MODES.mysql:
            self._session.sendline("quit")
        elif targetmode == CLI_MODES.pmx:
            self.setMode(CLI_MODES.config)
            self._session.sendline("pmx")
        elif targetmode == CLI_MODES.mysql:
            self.setMode(CLI_MODES.shell)
            self._session.sendline("idbmysql")
        elif targetmode != CLI_MODES.config and initialmode == CLI_MODES.pmx:
            # Moving from pmx to other modes. Switch to config and proceed..
            self.setMode(CLI_MODES.config)
            self.setMode(targetmode)
            self._session.sendline("")  # Send empty line for guessMode to work
        elif targetmode != CLI_MODES.shell and initialmode == CLI_MODES.mysql:
            # Moving from mysql to other modes. Switch to shell and proceed..
            self.setMode(CLI_MODES.shell)
            self.setMode(targetmode)
            self._session.sendline("")  # Send empty line for guessMode to work
        else:
            raise ValueError("Invalid Mode combination. Targetmode: %s, Currentmode: %s" % (targetmode, initialmode))

        finalmode = self.guessMode()
        logger.debug("Mode changed to %s mode" % finalmode)
        if targetmode == finalmode:
            if finalmode == CLI_MODES.shell:
                self.initShell()
            return True
        else :
            # A user can be in pmx subshells. So we might need to get back a couple levels
            if finalmode == CLI_MODES.pmx and targetmode == CLI_MODES.config:
                return self.setMode(CLI_MODES.config)
            else:
                logger.warn("Unable to set '%s' mode" % targetmode)
                return False
    
    def reboot(self, write_memory=True):
        """Reboots the machine, and waits till it comes back up"""
        self.resetStream()
        logger.info("Going to reboot %s" % self)
        self.setMode(CLI_MODES.config)  # Move to config to write memory
        if write_memory:
            logger.info("Saving configuration before rebooting..")
            self.sendCmd("write memory") 

        self._session.sendline("reload") 
        reboot_failed_tries = 3
        reboot_wait_tries = 3
        while True:
            i = self._session.expect([
                            "The system is going down for reboot",
                            "System shutdown initiated",
                            "Connection to [\.\d]* closed",
                            pexpect.EOF,
                            "Request failed",
                            pexpect.TIMEOUT,
                            ], timeout=120)
            if i == 0 or i == 1:
                logger.info("Reboot initiated")
                continue
            elif i == 2 or i == 3:
                logger.info("Machine Rebooted. Connection closed")
                break
            elif i == 4:
                if reboot_failed_tries > 0:
                    logger.info("Reboot failed. Trying again...")
                    self._session.sendline("reload") 
                    reboot_failed_tries -= 1
                    continue
            elif i == 5:
                if reboot_wait_tries > 0:
                    logger.warn("Waited for 120 secs, but machine did NOT reboot. Waiting for sometime more...")
                    reboot_wait_tries -= 1
                    continue
                else:
                    logger.error("Machine did NOT reboot!!!")
                    return False
            # break to prevent infinite loop
            break

        self._session.logfile_read.flush()
        self._session.logfile_read = None
        sys.stdout.flush()
        self.disconnect()
        logger.debug("Waiting for 300secs..")
        time.sleep(300)
        return self.waitTillReachable(180, timeout=1200)

    def poweroff(self, write_memory=True):
        """Powers off the machine"""
        logger.info("Going to power off %s" % self)
        self.resetStream()
        if write_memory:
            logger.info("Saving configuration before powering off")
            self.pushMode(CLI_MODES.config)  # Move to config to write memory
            self.sendCmd("write memory") 
            self.popMode()

        self.pushMode(CLI_MODES.shell)
        self._session.sendline("poweroff") 
        poweroff_failed_tries = 3
        poweroff_wait_tries = 3
        while True:
            i = self._session.expect([
                            "The system is going down",
                            "System shutdown initiated",
                            "Connection to [\.\d]* closed",
                            pexpect.EOF,
                            "Request failed",
                            pexpect.TIMEOUT,
                            ], timeout=120)
            if i == 0 or i == 1:
                logger.info("Shutdown initiated")
                continue
            elif i == 2 or i == 3:
                logger.info("Machine shutdown. Connection closed")
            elif i == 4:
                if poweroff_failed_tries > 0:
                    logger.info("Shutdown failed. Trying again...")
                    self._session.sendline("poweroff") 
                    poweroff_failed_tries -= 1
                    continue
            elif i == 5:
                if poweroff_wait_tries > 0:
                    logger.warn("Waited for 120 secs, but machine did NOT shutdown. Waiting for sometime more...")
                    poweroff_wait_tries -= 1
                    continue
                else:
                    logger.error("Machine did NOT shutdown!!!")
                    return False
            # break to prevent infinite loop
            break

        self._session.logfile_read.flush()
        return True

    def waitTillReachable(self, sleep_per_try_secs=120, timeout=1200):
        """Keeps pinging the node until it is reachable or timeout occurs."""
        elapsed_time = 0
        while elapsed_time < timeout:
            if self.isReachable():
                logger.info("Machine pingable. Reconnecting after 30 secs..")
                time.sleep(30)
                self.connect()
                return True
            else:
                logger.info("Machine not yet pingable. Waiting for %s secs before retrying.." % sleep_per_try_secs)
                time.sleep(sleep_per_try_secs)
            elapsed_time += sleep_per_try_secs
        logger.warning("TIMEOUT: Waited for %d secs, but machine still not reachable" % elapsed_time)
        return False

    def isReachable(self):
        """Pings the node and check if it is reachable"""
        cmd = "ping -c 1 %s" % self.ip
        ping_output = commands.getoutput(cmd)
        logger.debug(cmd)
        logger.debug(ping_output)
        return re.search("1[\s\w]+received", ping_output) is not None

    def isInCluster(self):
        """Check if the node is a part of a TM Cluster"""
        logger.debug("Checking if %s is a part of cluster" % self)
        role = self.getClusterRole()
        return role is not None and role != "DISABLED"

    def isMaster(self):
        """Check if the node is currently the master of TM Cluster"""
        logger.debug("Checking if %s is TM Master" % self)
        is_master = self.getClusterRole() == "MASTER"
        logger.debug("Is %s master: %s" % (self, is_master))
        return is_master

    def upgrade(self, imageurl):
        """Method to upgrade the image"""
        image_list = imageurl.split('/')
        image = image_list[-1]
        logger.info("Checking if the desired version is already installed")
        if self.checkInstalledVersion(image):
            logger.info("Desired image version is already installed. Skipping Upgrade..")
            return True

        bool = self.setMode(CLI_MODES.config)
        if not bool:
            return False

        output = self.sendCmd("image fetch %s" % imageurl, 3600)
        for err in image_fetch_errors:
            if re.search(err, output, re.I):
                logger.error("Image fetch failed")
                return False

        output = self.sendCmd("image install %s" % image , 3600)
        for err in image_install_errors:
            if re.search(err, output, re.I):
                logger.error("Image installation failed")
                return False

        self.sendCmd("image boot next")
        self.sendCmd("write memory")

        bool = self.reboot()
        if not bool:
            logger.error("Reboot failed")
            return False

        # Check if the newly installed version is the one we wanted to upgrade
        logger.info("Checking if the newly installed version is the desired one")
        if self.checkInstalledVersion(image):
            logger.info("Confirmed that the Installed version is equal to Desired version")
            return True
        else:
            logger.error("Installed version is not equal to Desired version")
            return False

    def checkInstalledVersion(self, image):
        """Check if a particular image is installed on the node"""
        self.pushMode(CLI_MODES.config)
        show_images_output = self.sendCmd("show images", ignoreErrors=True)
        read_version_line = False
        desired_version = ""
        for line in show_images_output.split("\n"):
            if read_version_line:
                desired_version = line.strip()
                break
            if line.strip() == image:
                read_version_line = True

        if not desired_version:
            logger.error("Unable to find the desired version line from show images output")
            self.popMode()
            return False
        else:
            logger.info("Desired Version: '%s'" % desired_version)

        installed_version = self.sendCmd("show version concise").strip()
        logger.info("Installed version: '%s'" % installed_version)
        self.popMode()
        if installed_version != desired_version:
            return False
        else:
            return True

    def applyPatch(self, patch_tar):
        logger.notice("Applying patches has not been implemented yet.")
        logger.notice("Please take out some time to implement `potluck.nodes.Node.applyPatch`")
        raise NotImplementedError("Patch application is not yet supported.")
        # Whoever is looking at this code, DO NOT USE this method as such. It used to be the (very) old way to apply patches.
        # TODO: Replace the below code to use pmx for applying patches
        head, tail = os.path.split(patch_tar)
        patch, ext = os.path.splitext(tail)

        patch_dir_on_server = "/data/utils/potluck/%s" % patch
        self.sendCmd("mkdir -p %s" % patch_dir_on_server)

        self.copyFromLocal(patch, patch_dir_on_server)

    def _extractFileDetails(self, filename):
        last_char = filename[-1]
        filetype = ""
        if last_char in ("*", "/", "=", ">", "@", "|", "%"):
            filename = filename[:-1]
            if last_char == "/":
                filetype = "directory"
            elif last_char == "@":
                filetype = "link"
            elif last_char == "=":
                filetype = "socket"
            elif last_char == "%":
                filetype = "whiteout"
            elif last_char == "|":
                filetype = "pipe"
            else:
                filetype = "file"
        else:
            filetype = "file"
        return filename, filetype

    def listDirectory(self, directory):
        """Returns the list of contents of a directory

        :param directory: The path of the directory to be listed
        :returns: List of dict in the format {"filename" : filename, "type" : filetype}
        """
        self.pushMode(CLI_MODES.shell)
        output = self.sendCmd("ls -F %s" % directory)
        self.popMode()
        if "No such file" in output:
            return []
        output_list = []
        for filename in output.split():
            filename, filetype = self._extractFileDetails(filename)
            output_list.append({
                "filename" : filename,
                "type" : filetype,
            })
        return output_list

    def createDirectory(self, directory):
        """Creates a directory on the machine

        :param directory: The path of the directory to be created
        """
        self.pushMode(CLI_MODES.shell)
        output = self.sendCmd("mkdir -p %s" % directory)
        self.popMode()
        return output

    def removePath(self, path):
        """Removes a file/directory from the machine

        :param path: The path of the file/directory to be removed

        .. warning::

            This method will forcefully remove (`rm -rf`) the path. Be Careful while calling this method.
        """
        self.pushMode(CLI_MODES.shell)
        output = self.sendCmd("rm -rf %s" % path)
        self.popMode()
        return output

    def put(self, *args, **kwargs):
        """Transfers a file from local machine to the server using :meth:`~.Node.scp`. An alias to :meth:`~.Node.copyFromLocal`"""
        self.copyFromLocal(*args, **kwargs)

    def get(self, *args, **kwargs):
        """Transfers a file from remote server to locaal machine using :meth:`~.Node.scp`. An alias to :meth:`~.Node.copyToLocal`"""
        self.copyToLocal(*args, **kwargs)

    def copyFromLocal(self, local_src, remote_dest):
        """Transfers a file from local machine to the server using :meth:`~.Node.scp`"""
        if not local_src.startswith("/"):
            # If this is not an absolute path, consider it present in potluck's "remote" directory
            for d in (ROOT_DIR, REMOTE_DIR):
                potential_src = os.path.join(d, local_src)
                if os.path.exists(potential_src):
                    local_src = potential_src
                    break
                else:
                    logger.warning("Path '%s' does not exist on local machine" % potential_src)
            else:
                logger.error("Path '%s' does not exist on local machine" % local_src)
                raise ValueError("Path '%s' does not exist on local machine" % local_src)

        # Create parent directory on the remote machine
        remote_parent_dir = os.path.dirname(remote_dest)
        self.createDirectory(remote_parent_dir)

        logger.info("Copying '%s' from local machine  to %s at '%s'" % (local_src, self.ip, remote_dest))
        return self.scp(local_src, "admin@%s:%s" % (self.ip, remote_dest))

    def copyToLocal(self, remote_src, local_dest):
        """Transfers a file from remote server to local machine using :meth:`~.Node.scp`"""
        if not local_dest.startswith("/"):
            local_dest = os.path.join(REMOTE_DIR, local_dest)
        logger.info("Copying '%s' from '%s' to local machine at '%s'" % (remote_src, self.ip, local_dest))
        parent_dir = os.path.dirname(local_dest)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        return self.scp("admin@%s:%s" % (self.ip, remote_src), local_dest)

    def scp(self, src, dst):
        """Transfers a file to the remote machine using scp

        .. note::

            This method is internally used by copyFromLocal and copyToLocal methods
        """
        scp_cmd = "scp -r %s %s" % (src, dst)
        connect_handle = pexpect.spawn(scp_cmd)
        connect_handle.setwinsize(400,400)
        connect_handle.logfile_read = sys.stdout
        i = 0
        ssh_newkey = r'(?i)Are you sure you want to continue connecting'
        while True:
            i = connect_handle.expect([ssh_newkey, 'Password: ', pexpect.EOF, pexpect.TIMEOUT])
            if i==0:
                connect_handle.sendline('yes')
                continue
            elif i==1:
                 #logger.info("Password supplied")
                 connect_handle.sendline(self.password)
                 continue
            elif i==2:
                logger.info("Scp complete")
                logger.info(connect_handle.before)
                break
            elif i==3:
                logger.warning("Timeout while waiting for connection")
                logger.info(connect_handle.before) # print out the result
                raise ValueError("Unable to establish connection")
        return True

    def getEpochTime(self, time_str=None):
        """Executes ``date +%s`` command on the node and gets the system time in epoch format

        :param time_str: Get time for this particular datetime string
        :returns: System time in epoch format
        :rtype: float
        """
        self.pushMode(CLI_MODES.shell)
        if time_str is None:
            system_time = self.sendCmd(r"date +%s")
        else:
            system_time = self.sendCmd("date -d '%s' " % time_str + r"+%s")

        try:
            system_time = float(system_time)
            logger.info("Time %f" % system_time)
        except:
            logger.error("Invalid system time '%s' on node '%s'" % (system_time, self))
            raise ValueError("Invalid system time '%s' on node '%s'" % (system_time, self))
        self.popMode()
        return system_time

    def getClusterRole(self):
        """Returns this node's role in TM Cluster

        :returns: Node role in upper-case
        """
        cur_mode = self.getMode()
        if cur_mode in (CLI_MODES.config, CLI_MODES.enable):
            output = self.sendCmd("show cluster local")
        elif cur_mode == CLI_MODES.shell:
            output = self.sendCmd("cli -m config -t 'show cluster local'")
        else:
            self.pushMode(CLI_MODES.config)
            output = self.sendCmd("show cluster local")
            self.popMode()

        match = re.search("Node\s*Role\s*:\s*(?P<ROLE>\S+)", output, flags=re.I)
        if match:
            return match.group("ROLE").upper()
        else:
            logger.error("Unable to get node role from `show cluster local`")
            return None
