import tagatagdb

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

db = tagatagdb.TagaTagDB("mydb")

import os
from os.path import join, basename, abspath, normpath, dirname

def unzip(l, n):
  return [i[n] for i in l]

def readnormlink(n):
  try:
    #dirname(str(n))
    #os.readlink(str(n))
    return normpath( abspath( join( dirname(str(n)), os.readlink(str(n)) ) ) )
  except Exception, e:
    #print e
    return normpath( abspath( n ) )

def InsertDirectory(path): 
  firstrootNode = db.AddNode( basename(path), normpath(abspath(path)) )
  pathlinks = []

  for root, dirs, files in os.walk(path):
    rootNode = db.GetNodeByContentPath(normpath(abspath(root)))

    for l in (dirs, files):
      for n in list(l):
        p = join(root, n)
        if islink(p):
          pathlinks.append( ( root, normpath(join(root, os.readlink(p))) ) )
          l.remove(n)

    newNodes = []
    for name in files + dirs:
      newNodes.append( db.AddNode( name, normpath(abspath(join(root, name))) ) )

    for node in newNodes:
      db.AddLink( rootNode, node )
  
  print pathlinks
  for frm, to in pathlinks:
    print to, readnormlink(to), frm, readnormlink(frm)
    db.AddLink( db.GetNodeByContentPath(readnormlink(frm)), db.GetNodeByContentPath(readnormlink(to)) )
    

def MakeSymlinkTree(path):
  renames = {}

  def nodeDirName(node):
    return node.GetName()+"~"+str(node.GetNID())  

  for node in db.GetNodes():
    dname = join(path, nodeDirName(node))
    indname = join(dname, "...")
    print "makedirs", indname
    os.makedirs( indname )

    contentPath = abspath(node.GetContentPath())
    print "symlink", contentPath, "content"
    os.symlink( contentPath, join(dname, "content") )

    for outnode in node.GetOutNodes():
      target = join(os.pardir, nodeDirName(outnode))
      source = join( dname, outnode.GetName() )
      print "symlink", target, source
      os.symlink( target, source )      

    for innode in node.GetInNodes():
      target = join(os.pardir, os.pardir, nodeDirName(innode))
      source = join( indname, innode.GetName() )
      print "symlink", target, source
      os.symlink( target, source )      

import yapgvb

def MakeGraph():
  graph = yapgvb.Digraph("Tags")
  nodes = {}

  for node in db.GetNodes():
    nodes[node.GetNID()] = graph.add_node( str(node.GetNID()), label=str(node.GetName()))

  for (frm, to) in db.GetLinks():
    graph.add_edge( nodes[frm.GetNID()], nodes[to.GetNID()] )

  return graph

InsertDirectory( "test" )
db.commit()

MakeSymlinkTree( "linktree" )

g = MakeGraph()
g.layout(yapgvb.engines.dot)
g.write()
g.render('out.svg')

