#!/usr/bin/env python
#
#    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#


from fuse import Fuse
import os
from errno import *
from stat import *

import os.path
import threading, thread

import tagatagdb

class NodePath:
  byNidDirName = "...by_nid"
  inLinksDirName = "..."
  contentLinkName = "content"

  IN = ("in",)
  OUT = ("out",)
  CONTENT = ("content",)
  SPECIAL = ("special",)

  def __init__( self, db, path, rootnodenid = 1 ):
    assert isinstance( path, str )
    #assert isinstance( rootnodenid,  )
    
    parts = path.split(os.sep)
    assert parts[0] == ""
    parts = parts[1:]
    print parts

    def rootLevelInLinkDir(l):
      assert l[0] == NodePath.inLinksDirName
      return {"nid": rootnodenid, "direction": NodePath.IN}
    def rootLevelOutLinkDir(l):
      return {"nid": rootnodenid, "direction": NodePath.OUT}
    def rootLevelInLink(l):
      assert l[0] == NodePath.inLinksDirName
      return {"nid": rootnodenid, "direction": NodePath.IN, "destName": l[1]}
    def rootLevelLink(l):
      return {"nid": rootnodenid, "direction": NodePath.OUT, "destName": l[0]}
      
    def byNidDir(l):
      assert l[0] == NodePath.byNidDirName
      return {"nid": None, "direction": NodePath.SPECIAL}
    def nidDir(l):
      assert l[0] == NodePath.byNidDirName
      return {"nid": long(l[1]), "direction": NodePath.OUT}
    def outLink(l):
      assert l[0] == NodePath.byNidDirName
      return {"nid": long(l[1]), "direction": NodePath.OUT, "destName": l[2]}
    def content(l):
      assert l[0] == NodePath.byNidDirName
      assert l[2] == NodePath.contentLinkName
      return {"nid": long(l[1]), "direction": NodePath.CONTENT}
    
    def inLinkDir(l):
      assert l[0] == NodePath.byNidDirName
      assert l[2] == NodePath.inLinksDirName
      return {"nid": long(l[1]), "direction": NodePath.IN}
    def inLink(l):
      assert l[0] == NodePath.byNidDirName
      assert l[2] == NodePath.inLinksDirName
      return {"nid": long(l[1]), "direction": NodePath.IN, "destName": l[3]}

    ANY = ("any",)

    matchSet = [
        ([""], rootLevelOutLinkDir),
        ([NodePath.inLinksDirName], rootLevelInLinkDir),
        ([NodePath.inLinksDirName, ANY], rootLevelInLink),
        ([NodePath.byNidDirName], byNidDir),
        ([NodePath.byNidDirName, ANY], nidDir),
        ([NodePath.byNidDirName, ANY, NodePath.contentLinkName], content),
        ([NodePath.byNidDirName, ANY, NodePath.inLinksDirName], inLinkDir),
        ([NodePath.byNidDirName, ANY, NodePath.inLinksDirName, ANY], inLink),
        ([NodePath.byNidDirName, ANY, ANY], outLink),
        ([ANY], rootLevelLink)
      ]

    self.destName = None
    
    for match in matchSet:
      if len(match[0]) == len(parts):
        matches = True
        for part_str, match_str in zip(parts, match[0]):
          if match_str is not ANY and part_str != match_str:
            matches = False
            break
        if matches:
          self.exists = True
          self.__dict__.update( match[1]( parts ) )
          break
        else:
          self.exists = False

    self.srcNode = None
    self.destNode = None
    
    try:
      if self.direction is not NodePath.SPECIAL:
        self.srcNode = db.GetNodeById(self.nid)
        self.srcNode.GetName() # force data load to varify the node exists
    
      if self.exists and self.srcNode is not None:
        if self.direction is NodePath.OUT and self.destName is not None:
          self.destNode = self.srcNode.GetOutNodeByName(self.destName)
        elif self.direction is NodePath.IN and self.destName is not None:
          self.destNode = self.srcNode.GetInNodeByName(self.destName)
        elif self.direction is NodePath.CONTENT:
          self.destNode = NodePath.CONTENT
        else:
          self.destNode = None
    except KeyError:
      self.exists = False
    

    print self.__dict__

"""
      if parts[0] != byNidDirName:
        basenid = rootnodenid
      else:
        assert parts[0] == byNidDirName
        try:
          basenid = long(parts[1])
        except ValueError:
          return None
        parts = parts[2:]

      if parts[0] == inLinksDirName and basenid is not None:
        direction = self.IN
        parts = parts[1:]

      destname = parts[0]
"""


class TagaTag(Fuse):
  def __init__(self, *args, **kw):

    Fuse.__init__(self, *args, **kw)

    if 1:
        print "TagaTag:mountpoint: %s" % repr(self.mountpoint)
        print "TagaTag:unnamed mount options: %s" % self.optlist
        print "TagaTag:named mount options: %s" % self.optdict

    self._threadLocal = threading.local()

  def openDB( self ):
    try:
      return self._threadLocal.db
    except AttributeError:
      print "Creating new TagaTagDB for thread", thread.get_ident()
      self._threadLocal.db = tagatagdb.TagaTagDB( self.optdict["db"] )
      return self._threadLocal.db

  #db = property( fget=getDB )

  multithreaded = 0
  flags = 0

  @staticmethod
  def nodeFromPath( db, path ):
    assert path.startswith( "/._nid/" )
    part = path[7:]
    nid = long( part.partition("/")[0] )
    return tagatagdb.Node(nid, db)

  def getattr(self, path):
    """
    Returns a tuple with the following elements:
      st_mode (protection bits)
      st_ino (inode number)
      st_dev (device)
      st_nlink (number of hard links)
      st_uid (user ID of owner)
      st_gid (group ID of owner)
      st_size (size of file, in bytes)
      st_atime (time of most recent access)
      st_mtime (time of most recent content modification)
      st_ctime (platform dependent; time of most recent metadata change on Unix, or the time of creation on Windows)
    """
    print "getattr", path
    #node = self.nodeFromPath(path)

    np = NodePath(self.openDB(), path)
    
    if not np.exists:
      return -ENOENT
    
    tp = 0

    if np.destNode is not None:
      tp = S_IFLNK
    else:
      tp = S_IFDIR

    st_mode = 0777 | tp # octal
    st_ino = 1
    st_dev = 0
    st_nlink = 1
    st_uid = 0
    st_gid = 0
    st_size = 0
    st_atime = 0
    st_mtime = 0
    st_ctime = 0

    return (st_mode,   st_ino,   st_dev,   st_nlink,   st_uid,   st_gid,   st_size,   st_atime,   st_mtime,   st_ctime)

  def readlink(self, path):
    print "readlink", path
    np = NodePath(self.openDB(), path)
    #db = self.openDB()
    
    if np.direction is NodePath.CONTENT:
      print "content link:", np.srcNode.GetContentPath()
      return str(np.srcNode.GetContentPath())
    
    assert np.destNode is not None
    
    return str( os.path.join( *(((os.pardir,)*(path.count("/")-1)) + (NodePath.byNidDirName, str(np.destNode.GetNID()))) ) )

  def getdir(self, path):
    print "getdir", path
    np = NodePath(self.openDB(), path)
    
    def nodeList2RetList( nodes ):
      return [(str(n.GetName()), 0) for n in nodes]

    if np.direction is NodePath.IN:
      return nodeList2RetList(np.srcNode.GetInNodes())
    elif np.direction is NodePath.OUT:
      ret = [(NodePath.inLinksDirName, 0)]
      if path != "/":
        ret.append( (NodePath.contentLinkName, 0) )
      ret += nodeList2RetList(np.srcNode.GetOutNodes())
      return ret
    
    return -EINVAL

  def unlink(self, path):
    return -EINVAL

  def rmdir(self, path):
    return -EINVAL

  def symlink(self, path, path1):
    print "symlink", path, path1
    db = self.openDB()
    np = NodePath( db, path )
    np1 = NodePath( db, path1 )
    db.AddLink(np1.srcNode, np.srcNode)
    return 0

  def rename(self, path, path1):
    return self.symlink(path, path1)

  def link(self, path, path1):
    print "link", path, path1
    db = self.openDB()
    np = NodePath( db, path )
    np1 = NodePath( db, path1 )
    db.AddLink(np.srcNode, np1.srcNode)
    return 0
    #return -EINVAL

  def chmod(self, path, mode):
    return -EINVAL

  def chown(self, path, user, group):
    return -EINVAL

  def truncate(self, path, size):
    return -EINVAL

  def mknod(self, path, mode, dev):
    return -EINVAL

  def mkdir(self, path, mode):
    print "TagaTag:mkdir: %s %x" % (path, mode)
    db = self.openDB()
    np = NodePath( db, path )
    node = db.AddNode(os.path.basename(path), None)
    db.AddLink(np.srcNode, node)
    return 0

  def utime(self, path, times):
    print "utime", path, times
    return -EINVAL

  def open(self, path, flags):
    print "TagaTag:open: %s" % path
    return -EINVAL

  def read(self, path, length, offset):
    print "TagaTag:read: %s" % path
    return -EINVAL

  def write(self, path, buf, off):
    print "TagaTag:write: %s" % path
    return -EINVAL

  def release(self, path, flags):
    print "TagaTag:release: %s %s" % (path, flags)
    return 0

  def statfs(self):
    """
    Should return a tuple with the following 6 elements:
        - blocksize - size of file blocks, in bytes
        - totalblocks - total number of blocks in the filesystem
        - freeblocks - number of free blocks
        - totalfiles - total number of file inodes
        - freefiles - nunber of free file inodes

    Feel free to set any of the above values to 0, which tells
    the kernel that the info is not available.
    """
    print "TagaTag:statfs:", self.openDB().GetNodeCount(), type(self.openDB().GetNodeCount())
    blocks_size = 1024
    blocks = self.openDB().GetNodeCount()
    blocks_free = 9223372036854775807 - blocks
    files = self.openDB().GetNodeCount()
    files_free = 9223372036854775807 - files
    namelen = 80
    return (blocks_size, blocks, blocks_free, files, files_free, namelen)

  def fsync(self, path, isfsyncfile):
    print "TagaTag:fsync: path=%s, isfsyncfile=%s" % (path, isfsyncfile)
    return 0


if __name__ == '__main__':

  server = TagaTag()
  server.multithreaded = 1;
  server.main()
