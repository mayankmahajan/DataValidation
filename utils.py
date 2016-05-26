"""
.. moduleauthor:: Sandeep Nanda <mail: sandeep.nanda@guavus.com> <skype: snanda85>

This module implements the utility functions used by the Potluck framework.

Most of the functions are used internally by the framework, although they
can be used anywhere

Example::

    from potluck import utils
    
    ls_output = utils.run_cmd("ls /tmp")
"""

from __future__ import with_statement   # For jython compatibility
import os
import re
import subprocess
import sys
import platform as py_platform
import ConfigParser
import xml.etree.cElementTree as eTree

from potluck.logging import logger
from settings import ROOT_DIR, SUITES_DIR, SCRIPTS_DIR, MODULES_DIR, TESTBEDS_DIR, USER_TESTBEDS_DIR, USER_SUITES_DIR
from potluck import env

testcase_ids = {}

class NodeDict(dict):
    ## Tweaks Tweaks Tweaks ##
    # This is a dict that wraps a node object. It does the following tasks:
    # 1. If `handle` key is accessed and node is not connected, connect it
    # 2. Provides access to the members of the Node object (e.g. testbed["INSTA1"].ip)
    #    Be sure when to use this, as this will connect to the node before returning the value
    def __getitem__(self, key, *args, **kwargs):
        try:
            return super(NodeDict, self).__getitem__(key, *args, **kwargs)
        except KeyError:
            if key == "handle":
                from potluck.nodes import connect
                connect(self["alias"])
                return self["handle"]

    def __getattr__(self, name):
        if name in self:
            # Expose items of this dict as attributes
            return self[name]
        elif hasattr(self["handle"], name):
            # Provides access to the members of node object
            return getattr(self["handle"], name)
        else:
            raise AttributeError("Parameter '%s' is not present in node '%s'" % (name, self["alias"]))

def sendEmail(to, subject, body="", body_html=""):
    """Sends email to a list of people.
    The email is sent from potluck@guavus.com

    :argument to: List of recipient IDs. The trailing `@guavus.com` is optional.
    :argument subject: Subject of the email
    :argument body: Text Body of the email
    :argument body_html: HTML Body of the email
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    def make_guavus(mail_id):
        if not mail_id.endswith("guavus.com"):
            return "%s@guavus.com" % mail_id
        else:
            return mail_id

    to = map(make_guavus, to)

    logger.info("Sending email to %s" % to)
    msg = MIMEMultipart('alternative')

    if body:
        text_msg = MIMEText(body, "plain")
        msg.attach(text_msg)

    if body_html:
        html_msg = MIMEText(body_html, "html")
        msg.attach(html_msg)

    sender = "Potluck <potluck@guavus.com>"

    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(to)

    s = smtplib.SMTP('localhost')
    s.sendmail(sender, to, msg.as_string())
    s.quit()

class processed_stream(object):
    """Processes a stream and replaces multiple newlines with single.

    .. note::
        
        Used by Potluck to process sys.stdout. Don't use unless you know what you are doing
    """
    def __init__(self, stream):
        self.stream = stream

    def flush(self):
        self.stream.flush()

    def write(self, data):
        str = data.rstrip()
        str = str.replace("\r\n", "\n")
        str = str.replace("\r", "\n")
        self.stream.write("%s\n" % str)

def generate_testcase_id(script):
    """Auto-Generates testcase id based on the script"""
    script_prefix, ext = os.path.splitext(script)
    tc_id = re.sub("/", "-", script_prefix).title().strip("-")
    if tc_id in testcase_ids:
        testcase_ids[tc_id] += 1
        return "%s-%s" % (tc_id, testcase_ids[tc_id])
    else:
        testcase_ids[tc_id] = 1
        return tc_id

def parse_suite_text(suite_file):

    suite = {
        "testcases" : [],
        "config_file" : None,
        "mail_to" : None,
        "ui_url" : None,
        "name" : ""
    }
    # Read the Suite and Form a list of scripts to execute
    if not os.path.isfile(suite_file):
        suite_file = os.path.join(SUITES_DIR, suite_file)

    suite["name"] = os.path.split(suite_file)[-1]
    if not os.path.exists(suite_file):
        logger.error("Suite file does not exist at %r" % suite_file)
        return None

    # Parse the Test suite file
    with open(suite_file, "r") as suite_file_handle:
        for line in suite_file_handle:
            line = line.strip()

            # Skip empty lines and lines starting with #
            if not line or line[0] == "#":
                continue

            # Check if there is a variable defined in the test suite
            match = re.match("(?P<name>\w+)\s*=\s*(?P<value>.+)", line)
            if match:
                name = match.group("name").lower()
                value = match.group("value")
                if name in ("mailto", "mail_to"):
                    suite["mail_to"] = value
                elif name in ("configfile", "config_file"):
                    suite["config_file"] = value
                # Continue to the next line of the file
                continue

            tokenized_line = line.split()
            if len(tokenized_line) == 2:
                script = tokenized_line[0]
                rule = tokenized_line[1]
            else:
                script = line
                rule = None

            # Auto-generate a testcase id
            tc_id = generate_testcase_id(script)

            script_file = os.path.join(SCRIPTS_DIR, script)
            if os.path.exists(script_file):
                suite["testcases"].append({"script" : script_file, "rule" : rule, "id" : tc_id, "description" : None})
            else:
                logger.error("Script does not exist %r" % script_file)
                return None
    return suite

def parse_suite_xml(suite_name, relative_paths=False):
    """Reads the xml suite file and Form a list of scripts to execute

    .. note::
        
        Used by Potluck to parse the test suite file
    """
    from settings import SUITES_DIR, SCRIPTS_DIR

    suite = {
        "sections" : [
            #{
            #    "name" : "", 
            #    "testcases" : []
            #}
        ],
        "config_file" : None,
        "mail_to" : None,
        "ui_url" : None,
        "name" : ""
    }
    # Read the Suite and Form a list of scripts to execute
    if os.path.isfile(suite_name):
        suite_file = suite_name
    else:
        suite_file = os.path.join(SUITES_DIR, suite_name)

    # If not found in SUITES_DIR
    if not os.path.isfile(suite_file):
        logger.warning("Suite file does not exist at %r" % suite_file)
        suite_file = os.path.join(USER_SUITES_DIR, suite_name)

    # Form the suitename from the path and filename
    suite["name"] = os.path.split(suite_file)[-1]
    if not os.path.exists(suite_file):
        logger.error("Suite file does not exist at %r" % suite_file)
        return None

    tree = eTree.parse(suite_file)
    suite_root = tree.getroot()

    global_section = {
        "name" : "Global",
        "testcases" : []
    }

    # Parse the Test suite file
    for current_tag in suite_root.getchildren():
        if current_tag.tag.lower() == "section":
            # Parse the section xml
            section = parse_section(current_tag, relative_paths)

            if section is None:
                return None
            else:
                suite["sections"].append(section)
        elif current_tag.tag.lower() == "testcase":
            # For backward compatibility, If there is a testcase without a section,
            # Then use a predefined global section
            #logger.warn("There is a testcase without section. This will be executed at the end")
            testcase = parse_testcase(current_tag, relative_paths)
            if testcase != None:
                global_section["testcases"].append(testcase)
            else:
                logger.error("Unable to Parse testcase")
                return None
        elif current_tag.tag.lower() == "suite":
            # If a nested suite is defined, parse it and get its scripts
            inner_suite_file = current_tag.text.strip()
            inner_suite = parse_suite_xml(inner_suite_file)
            # Extend the sections of the parent suite
            suite["sections"].extend(inner_suite["sections"])
        elif current_tag.tag.lower() in ("mailto", "mail_to"):
            suite["mail_to"] = current_tag.text.strip()
        elif current_tag.tag.lower() in ("configfile", "config_file"):
            suite["config_file"] = current_tag.text.strip()
        elif current_tag.tag.lower() == "config":
            # Read the config parameters associated with the suite
            for param in current_tag:
                env.config.set(param.tag, "".join([param.text] + [eTree.tostring(e) for e in param.getchildren()]).strip(), "SUITE")  # Set the parameters in suite context
        elif current_tag.tag.lower() == "ui_url":
            suite["ui_url"] = current_tag.text.strip()

    # If there is something in the global section, add it to the suite
    if global_section["testcases"]:
        suite["sections"].append(global_section)
    return suite

def parse_section(root, relative_paths=False):
    section_name = root.get("name")
    if not section_name:
        logger.error("`name` not defined for section")
        return None

    # Parse the section xml
    testcases = []
    for current_tag in root:
        if current_tag.tag.lower() == "testcase":
            testcase = parse_testcase(current_tag, relative_paths)
            if testcase != None:
                testcases.append(testcase)
            else:
                logger.error("Unable to Parse section")
                return None
        elif current_tag.tag.lower() == "suite":
            # If a nested suite is defined, parse it and get its scripts
            inner_suite_file = current_tag.text.strip()
            inner_suite = parse_suite_xml(inner_suite_file)
            # Extend the scripts of the current section
            for section in inner_suite["sections"]:
                testcases.extend(
                    section.get("testcases", [])
                )
        elif current_tag.tag.lower() == "config":
            # Read the config parameters associated with the suite
            for param in current_tag:
                # Set the parameters in section context
                env.config.set(param.tag, "".join([param.text] + [eTree.tostring(e) for e in param.getchildren()]).strip(), "SECTION_%s" % section_name)
        elif current_tag.tag.lower() == "section":
            logger.error("Nested Sections are not supported in testsuite xml")
            raise ValueError("Nested Sections are not supported in testsuite xml")

    return {
        "name" : section_name,
        "testcases" : testcases,
    }

def parse_testcase(root, relative_paths=False):
    script_tag = root.find("script")
    script = None
    rule = None
    description = root.get("description")

    # Script tag is mandatory
    if script_tag is None:
        logger.error("`script` tag is not defined for test: '%s [%s]'" % (root.tag, root.attrib))
        return None
    else:
        script = script_tag.text.strip()

    # If explicit TC-Id is not mentioned in the test suite,
    # form out a TC id from the script path by replacing all the slashes with hyphens
    tc_id = generate_testcase_id(script)

    for current_tag in root:
        tagname = current_tag.tag.lower()   # Tagname
        text = current_tag.text.strip() # Text associated with the tag
        if tagname in ("onfailure", "on_failure"):
            rule = text
        elif tagname == "config":
            # Read all the config variables associated with the testcase
            for param in current_tag:
                env.config.set(param.tag, "".join([param.text] + [eTree.tostring(e) for e in param.getchildren()]).strip(), tc_id)  # Set the parameters in testcase context
        elif tagname == "description":
            # Description can be an attribute as well as a tag also
            description = text

    if script.startswith("/"):
        script_file = script
    else:
        script_file = os.path.abspath(os.path.join(SCRIPTS_DIR, script))

    if not os.path.exists(script_file):
        logger.error("Script does not exist %r" % script_file)
        raise ValueError("Script does not exist %r" % script_file)

    if relative_paths is True:
        script_file = os.path.relpath(script_file, SCRIPTS_DIR)
    return {"script" : script_file, "rule" : rule, "id" : tc_id, "description" : description}
        

def run_cmd(cmd, async=False, shell=None):
    """Executes a command on the local machine

    :argument cmd: The command to be executed
    :returns: Output of the executed command including stdout and stderr
    """
    if shell is None:
        p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    else:
        p = subprocess.Popen(cmd, executable=shell, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    if async is True:
        return p
    else:
        stdout, stderr = p.communicate()
        return stdout.strip()

def platform():
    """Returns the platform on which Potluck is running

    :returns: Either one of OSX, WIN, LINUX
    """
    platform_str = py_platform.platform().lower()
    if "mac" in platform_str or "darwin" in platform_str:
        return "OSX"
    elif "windows" in platform_str or "win32" in platform_str:
        return "WIN"
    else:
        return "LINUX"

def parse_testbed(tb_name):
    if os.path.isfile(tb_name):
        tb_file = tb_name   # If absolute path is passed
    else:
        # If absolute path is not passed, find in TESTBEDS_DIR
        tb_file = os.path.join(TESTBEDS_DIR, tb_name)

    # If not found in TESTBEDS_DIR
    if not os.path.isfile(tb_file):
        logger.warning("Testbed file does not exist at %r" % tb_file)
        tb_file = os.path.join(USER_TESTBEDS_DIR, tb_name)

    # Parse the testbed file
    if not os.path.exists(tb_file):
        logger.error("Testbed file does not exist at %r" % tb_file)
        return {}

    testbed = {}
    default_testbed_values = {
        "connection" : "ssh",
        "port" : "22",
        "serviceport" : 22222,
    }
    tb = ConfigParser.RawConfigParser(defaults=default_testbed_values)
    tb.read(tb_file)

    # Python version 2.7: The default dict_type of ConfigParser is collections.OrderedDict.
    # So, the sections are ordered. But this will not be true for versions < 2.7
    for section in tb.sections():
        node = NodeDict()
        for key, value in tb.items(section):
            node[key] = value

        node_alias = section
        node["alias"] = node_alias
        node["type"] = [t.strip() for t in node["type"].upper().split(",")]    #List of types

        testbed[node_alias] = node
    return testbed

def has_ui_testcases(suite):
    """Returns True if there is atleast one sikuli testcases present in the suite

    :argument suite: parsed suite (as returned by :func:`parse_suite_xml`)
    """
    for section in suite["sections"]:
        for testcase in section["testcases"]:
            if "sikuli" in testcase["script"]:
                return True
    return False

def csv_diff(csv1_file, csv2_file, delimiter=",", error_percent_plus=1, error_percent_minus=1, key_columns=[1]):
    """Calculates the diff between any two CSVs, based on their values
    """
    diff_str = ""

    # Load the first file in a dict
    csv1_dict = {}
    ##### INSTA CSV ######
    with open(csv1_file, "rU") as csv1_fd:
        for line in csv1_fd:
            line = line.strip()
            fields = line.split(delimiter)
            if "itrate" in line or "rowth" in line  or "imestamp" in line or  "Total" in line or "Traffic" in line or "(bps)" in line or  not line:
                continue
            elif line.startswith("All") or line.startswith("Total"):
                key = "TOTAL_ROW"
            else:
                key = tuple([fields[kc-1].upper() for kc in key_columns])

            values = [str(val) for idx, val in enumerate(fields, start=1) if idx not in key_columns]

            #if key in csv1_dict:
            #    logger.error("Key '%s' is present multiple times in the csv file. It will be overwritten." % key)
            csv1_dict[key] = (values, line)

    # Read the second file and start comparing with the first one
    ##### UI CSV #####
    with open(csv2_file, "rU") as csv2_fd:
        for line in csv2_fd:
            line = line.strip()
            fields = line.split(delimiter)
            if "itrate" in line or "rowth" in line or "imestamp" in line or  "Total" in line or "Traffic" in line or "(bps)" in line or not line:
                continue
            elif line.startswith("All") or line.startswith("Total"):
                key = "TOTAL_ROW"
            else:
                key = tuple([fields[kc-1].upper() for kc in key_columns])

            csv2_values = []
 
            for idx, val in enumerate(fields, start=1):
                if idx not in key_columns :
                    if 'NA' not in val :
                        csv2_values.append(float(val)) 
                    else: 
                        csv2_values.append(str("NA"))
            
            #csv2_values = [ float(val) for idx, val in enumerate(fields, start=1) if idx not in key_columns ]
        

            try:
                csv1_values, csv1_line = csv1_dict[key]
                del csv1_dict[key]  # If the values are found, remove this key. The remaining values will be displayed later
            except KeyError:
                diff_str += "+%s\n" % line    # Key is present in csv2, but not in csv1
                continue

            for csv1_val, csv2_val in zip(csv1_values, csv2_values):
		if csv1_val == 'NA' and csv2_val == 'NA':
                                continue
                elif  abs(float(csv1_val) - float(csv2_val)) > 10000 :
                    if float(csv1_val) != 0 :
                        difference = (float(csv1_val) - float(csv2_val)) * 100 / float(csv1_val)
                    elif float(csv2_val) != 0 :
                        difference = (float(csv1_val) - float(csv2_val)) * 100 / float(csv2_val)
                    else:
                        difference = 0
                else:
                    difference = 0

                if (difference > 0 and difference > error_percent_plus) or \
                    (difference < 0 and abs(difference) > error_percent_minus):
                    # Difference in the values is greater than allowed difference
                    diff_str += "<%s\n" % csv1_line
                    diff_str += ">%s\n" % line
                    break

    # Display the lines present in CSV1 but not present in CSV2
    for k, v in csv1_dict.iteritems():
       	diff_str += "-%s\n" % v[1]
    return diff_str

def replace_variables_in_template(template, variables, output_file=None):
    """Reads an input template file and replace given variables with their values

    :param template: Path to input template file. The file can be present in ROOT_DIR, SUITES_DIR or SCRIPTS_DIR
    :param variables: A dict with all variables to replace and their values
    :param output_file: *[Optional]* Path to output file
    """
    from string import Template

    # Order of preference to lookup finding the template file.
    for d in (ROOT_DIR, SUITES_DIR, SCRIPTS_DIR):
        template_file = os.path.join(d, template)

        if os.path.isfile(template_file):
            logger.debug("Using configure template file '%s'." % template_file)
            break
        else:
            logger.warning("Template file does not exist at '%s'." % template_file)
    else:
        logger.error("Configure template '%s' does not exist" % template)
        return False

    # Read the file as a string template
    try:
        template = Template(open(template_file, "rU").read())
    except:
        logger.exception("Unable to parse the template file")
        return False

    # Substitute the variables in the template
    try:
        config = template.substitute(variables)
    except KeyError as e:
        logger.error("%s not found in config variables" % e)
        return False
    except:
        logger.exception("Invalid config template file")
        return False

    if output_file is not None:
        parent_dir = os.path.dirname(output_file)
        try:
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
        except:
            logger.exception("Error while creating parent directories")

        # Write the config to a temporary location on local disk
        with open(output_file, "w") as fh:
            fh.write(config)

    return config
