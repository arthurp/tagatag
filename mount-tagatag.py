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

import tagatagdb

class TagaTag(Fuse):

    def __init__(self, *args, **kw):
    
        Fuse.__init__(self, *args, **kw)
    
        if 1:
            print "TagaTag:mountpoint: %s" % repr(self.mountpoint)
            print "TagaTag:unnamed mount options: %s" % self.optlist
            print "TagaTag:named mount options: %s" % self.optdict

        self._db = None

    def openDB( self ):
      return tagatagdb.TagaTagDB( self.optdict["db"] )
    
    #db = property( fget=getDB )

    @staticmethod
    def nodeFromPath( db, path ):
      assert path.startswith( "/._nid/" )
      part = path[7:]
      nid = long( part.partition("/")[0] )
      return tagatagdb.Node(nid, db)
    
    flags = 1
    
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
      
      tp = 0

      if path == "/" or path == "/._nid" or path.endswith("..."):
        tp = S_IFDIR
      elif path.count("/") == 1:
        tp = S_IFLNK
      elif path.count("/") == 2:
        tp = S_IFDIR
      else:
        tp = S_IFLNK

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
      db = self.openDB()

      if path.count("/") == 1:
        node = db.GetNodeByName(path[1:])
        return os.path.join( "._nid", str(node.GetNID()) )
      else:
        node = self.nodeFromPath(db, path)
        if path.find("...") != -1:
          for n in node.GetInNodes():
            if n.GetName() == path.rpartition("/")[2]:
              return os.path.join( os.pardir, os.pardir, str(n.GetNID()) )
        elif path.endswith("/content"):
          print "content link:", node.GetContentPath()
          return str(node.GetContentPath())
        else:
          for n in node.GetOutNodes():
            if n.GetName() == path.rpartition("/")[2]:
              return os.path.join( os.pardir, str(n.GetNID()) )

    def getdir(self, path):
      print "getdir", path
      def nodeList2RetList( nodes ):
        return [(str(n.GetName()), 0) for n in nodes]

      db = self.openDB()

      if path == "/":
        return nodeList2RetList(db.GetNodes())
      elif path.find("...") != -1:
        node = self.nodeFromPath(db, path)
        return nodeList2RetList(node.GetInNodes())
      else:
        node = self.nodeFromPath(db, path)
        return [("...",0), ("content",0)] + nodeList2RetList(node.GetOutNodes())

    def unlink(self, path):
      return -EINVAL

    def rmdir(self, path):
      return -EINVAL

    def symlink(self, path, path1):
      db = self.openDB()
      db.AddLink(self.nodeFromPath(db, path), self.nodeFromPath(db, path1))      

    def rename(self, path, path1):
      return self.symlink(path, path1)

    def link(self, path, path1):
      return -EINVAL

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
      node = db.AddNode(os.path.basename(path), str(uuid.uuid4()))
      db.AddLink(self.nodeFromPath(path), node)
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
        print "TagaTag:statfs: returning fictitious values"
        blocks_size = 0
        blocks = 0
        blocks_free = 0
        files = seld.db.GetNodeCount()
        files_free = 0
        namelen = 80
        return (blocks_size, blocks, blocks_free, files, files_free, namelen)
    def fsync(self, path, isfsyncfile):
        print "TagaTag:fsync: path=%s, isfsyncfile=%s" % (path, isfsyncfile)
        return 0
    

if __name__ == '__main__':

  server = TagaTag()
  server.multithreaded = 1;
  server.main()
