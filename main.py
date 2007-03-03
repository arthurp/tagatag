from pysqlite2 import dbapi2 as sqlite

import uuid

import os
from os.path import isdir, islink

try:
  os.unlink( "mydb" )
except:
  pass

def rm_rf(top):
  """
  Delete everything reachable from the directory named in 'top',
  assuming there are no symbolic links.
  CAUTION:  This is dangerous!  For example, if top == '/', it
  could delete all your disk files.
  """
  for root, dirs, files in os.walk(top, topdown=False):
    for name in files + dirs:
      path = os.path.join(root, name)
      #print path, islink(path), isdir(path), islink(path) or not isdir(path)
      if islink(path) or not isdir(path):
        os.remove(path)
      else:
        os.rmdir(path)

try:
  rm_rf("linktree")
except Exception, e:
  print e

conn = sqlite.connect("mydb")

cur = conn.cursor()

def SchemeVersion(cur):
  try:
    cur.execute("select * from version_info")
    return cur.fetchone()[0]
  except:
    return 0

if SchemeVersion(cur) < 1:
  cur.execute("create table version_info (scheme integer)")
  cur.execute("insert into version_info (scheme) values (1)")
  cur.execute("create table links (itemid char(45) not null, tagid char(45) not null)")
  cur.execute("create table tags (itemid char(45) unique, name varchar(128) not null, path varchar(512) unique)")
  cur.execute("create table files (itemid char(45) unique, name varchar(128) not null, path varchar(512) unique)")

assert SchemeVersion(cur) == 1, "Wrong scheme!"

def UUID():
  return uuid.uuid4().urn

import os
from os.path import join, basename, abspath, normpath, dirname

def unzip(l, n):
  return [i[n] for i in l]

def readnormlink(n):
  try:
    #dirname(str(n))
    #os.readlink(str(n))
    return normpath( join( dirname(str(n)), os.readlink(str(n)) ) )
  except Exception, e:
    #print e
    return n

def InsertDirectory(cur, path): 
  cur.execute("insert into tags (itemid, name, path) values (?, ?, ?)", (UUID(),basename(path),path))
  dirpathlinks = []
  filepathlinks = []

  for root, dirs, files in os.walk(path):
    cur.execute("select itemid from tags WHERE path = ?", (root,))
    rootid = cur.fetchone()[0]

    for n in list(dirs):
      p = join(root, n)
      if islink(p):
        dirpathlinks.append( ( root, normpath(join(root, os.readlink(p))) ) )
        dirs.remove(n)
    for n in list(files):
      p = join(root, n)
      if islink(p):
        filepathlinks.append( ( root, normpath(join(root, os.readlink(p))) ) )
        files.remove(n)

    add_files = [(UUID(),name,join(root, name)) for name in files]
    cur.executemany("insert into files (itemid, name, path) values (?, ?, ?)", add_files)

    add_dirs = [(UUID(),name,join(root, name)) for name in dirs]
    cur.executemany("insert into tags (itemid, name, path) values (?, ?, ?)", add_dirs)

    #print root, rootid

    add_links = [(rootid, x) for x in unzip(add_files, 0) + unzip(add_dirs, 0)]
    #print add_links
    cur.executemany("insert into links (tagid, itemid) values (?, ?)", add_links )

  print dirpathlinks, filepathlinks
  for frm, to in dirpathlinks:
    print to, readnormlink(to), frm, readnormlink(frm)
    cur.execute("select itemid from tags WHERE path = ?", (readnormlink(frm),))
    frmid = cur.fetchone()[0]
    cur.execute("select itemid from tags WHERE path = ?", (readnormlink(to),))
    toid = cur.fetchone()[0]
    cur.execute("insert into links (tagid, itemid) values (?, ?)", (frmid, toid) )
  for frm, to in filepathlinks:
    print to, readnormlink(to), frm, readnormlink(frm)
    cur.execute("select itemid from tags WHERE path = ?", (readnormlink(frm),))
    frmid = cur.fetchone()[0]
    cur.execute("select itemid from files WHERE path = ?", (readnormlink(to),))
    toid = cur.fetchone()[0]
    cur.execute("insert into links (tagid, itemid) values (?, ?)", (frmid, toid) )
    

def MakeSymlinkTree(cur, path):
  cur.execute("SELECT name FROM tags")
  for (tag,) in cur:
    print "makedirs", join(path, tag)
    os.makedirs( join(path, tag) )

  cur.execute("SELECT files.path, files.name, tags.name FROM files, links, tags WHERE files.itemid = links.itemid AND links.tagid = tags.itemid")
  for fp, fn, tag in cur:
    print "symlink", abspath(fp), join(path, tag, fn)
    os.symlink( abspath(fp), join(path, tag, fn) )

  cur.execute("SELECT ftags.name, ttags.name FROM tags AS ftags, links, tags AS ttags WHERE ftags.itemid = links.itemid AND links.tagid = ttags.itemid")
  for to, frm in cur:
    print "symlink", join(os.pardir, to), join(path, frm, to)
    os.symlink( join(os.pardir, to), join(path, frm, to) )

import yapgvb

def MakeGraph(cur):
  graph = yapgvb.Digraph("Tags")
  nodes = {}

  def createNodes(cur, fillcolor):
    for itemid, name in cur:
      nodes[itemid] = graph.add_node( str(itemid), label=str(name), color=fillcolor )
    
  cur.execute("SELECT itemid, name FROM files")
  createNodes(cur, "blue")
  cur.execute("SELECT itemid, name FROM tags")
  createNodes(cur, "black")

  def createEdges(cur):
    for frm, to in cur:
      graph.add_edge( nodes[frm], nodes[to] )

  cur.execute("SELECT tags.itemid, files.itemid FROM files, links, tags WHERE files.itemid = links.itemid AND links.tagid = tags.itemid")
  createEdges(cur)
  cur.execute("SELECT ftags.itemid, ttags.itemid FROM tags AS ftags, links, tags AS ttags WHERE ttags.itemid = links.itemid AND links.tagid = ftags.itemid")
  createEdges(cur)

  return graph

InsertDirectory( cur, "test" )
conn.commit()

MakeSymlinkTree( cur, "linktree" )

g = MakeGraph(cur)
g.write()
g.layout(yapgvb.engines.dot)
g.render('out.svg')

