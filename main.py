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
  cur.execute("create table links (fromnode char(45) not null, tonode char(45) not null)")
  cur.execute("create table nodes (uuid char(45) unique, name varchar(128) not null, path varchar(512) unique, istag boolean not NULL)")

assert SchemeVersion(cur) == 1, "Wrong scheme!"

def UUID():
  return uuid.uuid1().urn

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
  cur.execute("insert into nodes (uuid, name, path, istag) values (?, ?, ?, ?)", (UUID(),basename(path),path, True))
  pathlinks = []

  for root, dirs, files in os.walk(path):
    cur.execute("select uuid from nodes WHERE path = ?", (root,))
    rootid = cur.fetchone()[0]

    for l in (dirs, files):
      for n in list(l):
        p = join(root, n)
        if islink(p):
          pathlinks.append( ( root, normpath(join(root, os.readlink(p))) ) )
          l.remove(n)

    add_files = [(UUID(),name,join(root, name), False) for name in files]
    cur.executemany("insert into nodes (uuid, name, path, istag) values (?, ?, ?, ?)", add_files)

    add_dirs = [(UUID(),name,join(root, name), True) for name in dirs]
    cur.executemany("insert into nodes (uuid, name, path, istag) values (?, ?, ?, ?)", add_dirs)

    #print root, rootid

    add_links = [(rootid, x) for x in unzip(add_files, 0) + unzip(add_dirs, 0)]
    #print add_links
    cur.executemany("insert into links (fromnode, tonode) values (?, ?)", add_links )

  print pathlinks
  for frm, to in pathlinks:
    print to, readnormlink(to), frm, readnormlink(frm)
    cur.execute("select uuid from nodes WHERE path = ?", (readnormlink(frm),))
    frmid = cur.fetchone()[0]
    cur.execute("select uuid from nodes WHERE path = ?", (readnormlink(to),))
    toid = cur.fetchone()[0]
    cur.execute("insert into links (fromnode, tonode) values (?, ?)", (frmid, toid) )
    

def MakeSymlinkTree(cur, path):
  renames = {}

  cur.execute("SELECT name, uuid FROM nodes WHERE istag = 1")
  for (tag,uid) in cur:
    print "makedirs", join(path, tag)
    try:
      os.makedirs( join(path, tag) )
      renames[uid] = tag
    except:
      os.makedirs( join(path, tag+"~"+uid) )
      renames[uid] = tag+"~"+uid

  cur.execute("SELECT fnodes.uuid, fnodes.name, tnodes.name, tnodes.path, tnodes.istag FROM nodes AS fnodes, links, nodes AS tnodes WHERE fnodes.uuid = links.fromnode AND links.tonode = tnodes.uuid")
  for frmid, frm, to, topath, toistag in cur:
    linkname = join(path, renames[frmid], to)
    if toistag:
      print "symlink", join(os.pardir, to), linkname
      os.symlink( join(os.pardir, to), linkname )
    else:
      print "symlink", abspath(topath), linkname
      os.symlink( abspath(topath), linkname )

import yapgvb

def MakeGraph(cur):
  graph = yapgvb.Digraph("Tags")
  nodes = {}

  cur.execute("SELECT uuid, name, CASE istag WHEN 1 THEN 'black' ELSE 'blue' END 'istag' FROM nodes")
  for itemid, name, color in cur:
    nodes[itemid] = graph.add_node( str(itemid), label=str(name), color=color, URL=itemid )

  cur.execute("SELECT fnodes.uuid, tnodes.uuid FROM nodes AS fnodes, links, nodes AS tnodes WHERE fnodes.uuid = links.fromnode AND links.tonode = tnodes.uuid")
  for frm, to in cur:
    graph.add_edge( nodes[frm], nodes[to] )

  return graph

InsertDirectory( cur, "test" )
conn.commit()

MakeSymlinkTree( cur, "linktree" )

g = MakeGraph(cur)
g.layout(yapgvb.engines.dot)
g.write()
g.render('out.svg')

