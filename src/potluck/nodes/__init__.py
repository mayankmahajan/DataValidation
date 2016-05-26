"""
.. moduleauthor:: Sandeep Nanda <mail: sandeep.nanda@guavus.com> <skype: snanda85>

Potluck provides some generic APIs to connect and manage nodes in test scripts

The framework uses the concept of Mixins to create Node objects. There is a separate
mixin for each type of node defined in the testbed.

Following node types(Case-Insensitive) are supported in a testbed file

* :class:`NAMENODE <potluck.nodes.NameNode.NameNodeMixin>`
* :class:`DATANODE <potluck.nodes.DataNode.DataNodeMixin>`
* :class:`INSTA <potluck.nodes.InstaNode.InstaMixin>`
* RGE
* RUBIX
* :class:`COLLECTOR <potluck.nodes.CollectorNode.CollectorMixin>`

"""

from potluck.logging import logger

testbed = {}
alias_list = []

class CLI_MODES:
    config = "config"
    enable = "enable"
    shell = "shell"
    pmx = "pmx"
    mysql = "mysql"

def connect_multiple(node_alias_list):
    """
    Given a list of node aliases, this function connects to all of them.
    It is better to use this function, whenever there is a need to do the same
    operations on multiple nodes.

    :argument node_alias_list: List of node aliases
    :returns: List of connected node handles
    """
    connected_nodes = []
    for node_alias in node_alias_list:
        connected_nodes.append(connect(node_alias))
    return connected_nodes

def connect(node_alias, force_new=False):
    """
    This function connects to the given node. It takes care
    of creating the correct node object based on the `type` of the
    node defined in the testbed

    :argument node_alias: Node alias defined in the testbed
    :argument force_new: Forces a new connection object, otherwise the framework will try to reuse an existing connection. This connection will not be stored for later use.
    :returns: Connected node object
    """
    from Node import Node
    from NameNode import NameNodeMixin
    from DataNode import DataNodeMixin
    from InstaNode import InstaMixin
    from UiNode import RgeMixin, RubixMixin
    from CollectorNode import CollectorMixin
    from ReNode import ReMixin
    from PsqlNode import PsqlMixin

    node_alias = node_alias

    if node_alias not in testbed:
        raise ValueError("Node %r not present in testbed" % node_alias)

    # Get the node dict from the testbed
    node = testbed[node_alias]

    # If the node is already connected, re-use the same handle
    # TODO: Figure out if this is a good idea
    #   Pros: Re-use the connection, increase execution speed
    #   Cons: Cannot create multiple connections to same node
    if force_new is not True and "handle" in node and node["handle"].isConnected() is True:
        logger.info("Re-using existing connection for %r" % node_alias)
        node["handle"].resetStream()
        return node["handle"]
    logger.info("Connecting to %r" % node_alias)

    # New class definition to allow Mixins
    class NodeClass(Node): pass

    node_types = node["type"]
    # Reversing the list to match the order of applying Mixins with the one mentioned in Testbed
    node_types.reverse()
    for node_type in node_types:
        node_type = node_type.strip()
        if node_type == "NAMENODE":
            Mixin(NodeClass, NameNodeMixin)
        elif node_type == "DATANODE":
            Mixin(NodeClass, DataNodeMixin)
        elif node_type == "INSTA":
            Mixin(NodeClass, InstaMixin)
        elif node_type == "RGE":
            Mixin(NodeClass, RgeMixin)
        elif node_type == "RUBIX":
            Mixin(NodeClass, RubixMixin)
        elif node_type == "COLLECTOR":
            Mixin(NodeClass, CollectorMixin)
        elif node_type == "RE":
            Mixin(NodeClass, ReMixin)
        elif node_type == "PSQL":
            Mixin(NodeClass, PsqlMixin)
        else:
            logger.warn("Invalid Node Type: %s." % node_type)

    if force_new is True:
        return NodeClass(node)
    else:
        node["handle"] = NodeClass(node)
        return node["handle"]

def Mixin(pyClass, mixInClass, makeLast=0):
  if mixInClass not in pyClass.__bases__:
    if makeLast:
      pyClass.__bases__ += (mixInClass,)
    else:
      pyClass.__bases__ = (mixInClass,) + pyClass.__bases__


def get_nodes_by_type(*args):
    """
    Returns the node aliases for the specified type

    :argument args: Any number of node types
    :returns: List of node aliases

    Example::

        from potluck.nodes import get_nodes_by_type
        collectors = get_nodes_by_type("Collector")
        collectors_and_insta = get_nodes_by_type("Collector", "insta")
    """
    nodes = []
    for type in args:
        type_nodes = [k for k, v in testbed.iteritems() if type.upper() in v["type"]]
        logger.debug("Number of %s nodes: %d" % (type, len(type_nodes)))
        nodes.extend(type_nodes)
    return nodes

def connect_if_required(node):
    from Node import Node
    # If this is not a `Node` object, assume that it is an alias and try to connect it
    if isinstance(node, Node):
        return node
    else:
        return connect(node)

def find_master(nodes):
    """Find the Master TM node among a list of nodes"""
    nodes = map(connect_if_required, nodes)
    for node in nodes:
        if node.isMaster():
            logger.debug("%s is TM Master" % node)
            return node

    logger.warning("None of the nodes is Master. Assuming first disabled node to be Master")
    for node in nodes:
        if node.isInCluster() is False:
            logger.debug("Considering '%s' as TM Master" % node)
            return node

    logger.error("Could not find any master from the cluster")
    return None

def find_master_by_type(node_type):
    """
    Returns the handle to the TM master from the cluster

    :argument node_type: Type of nodes to process
    :returns: Node object corresponding to the TM master node

    Example::

        from potluck.nodes import find_master_by_type

        # Connect to the master node
        master_namenode = find_master_by_type("namenode")

        # Do some tasks
        master_namenode.sendCmd("show version")
    """
    node_aliases = get_nodes_by_type(node_type)
    return find_master(node_aliases)
