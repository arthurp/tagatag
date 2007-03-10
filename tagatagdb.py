from pysqlite2 import dbapi2 as sqlite

class Node:
  def __init__( self, nid, db ):
    assert isinstance( nid, int ) or isinstance( nid, long )
    assert isinstance( db, TagaTagDB )

    self._nid = nid
    self._db = db

    self._name = None
    self._contentPath = None
  
  def GetInNodes( self ):
    """Return a generator for all the nodes in the database that link to this one. See TagaTagDB.GetNodes() for more information."""
    cur = self._db._conn.cursor()
    cur.execute("SELECT fromNode FROM edges WHERE toNode = ?", (self._nid,))
    for (nid,) in cur:
      yield Node(nid, self._db)
      
  def GetInNodeByName( self, name ):
    cur = self._db._conn.cursor()
    cur.execute("SELECT nid FROM edges INNER JOIN nodes ON fromNode = nid WHERE toNode = ? AND name = ? ORDER BY nid ASC", (self._nid, name))
    res = cur.fetchone()
    if res is None:
      raise KeyError(self._nid, name, "no in node with that name in the DB")
    return Node(res[0], self._db)

  def GetOutNodes( self ):
    """Return a generator for all the nodes in the database this one links to. See TagaTagDB.GetNodes() for more information."""
    cur = self._db._conn.cursor()
    cur.execute("SELECT toNode FROM edges WHERE fromNode = ?", (self._nid,))
    for (nid,) in cur:
      yield Node(nid, self._db)

  def GetOutNodeByName( self, name ):
    cur = self._db._conn.cursor()
    cur.execute("SELECT nid FROM edges INNER JOIN nodes ON toNode = nid WHERE fromNode = ? AND name = ? ORDER BY nid ASC", (self._nid, name))
    res = cur.fetchone()
    if res is None:
      raise KeyError(self._nid, name, "no out node with that name in the DB")
    return Node(res[0], self._db)


  def GetNID( self ):
    return self._nid

  def GetContentPath( self ):
    self._loadInfo()
    return self._contentPath

  def GetName( self ):
    self._loadInfo()
    return self._name

  def _loadInfo( self ):
    if self._name is None:
      cur = self._db._conn.cursor()
      cur.execute("SELECT name, contentPath FROM nodes WHERE nid = ?", (self._nid,))
      res = cur.fetchone()
      if res is None:
        raise KeyError(self._nid, "not an nid in the DB")
      self._name, self._contentPath = res
    assert self._name is not None
    assert self._contentPath is not None
  
  def __repr__( self ):
    return "<Node %d %r %r>" % (self._nid, self.GetName(), self.GetContentPath())

class TagaTagDB:
  def __init__( self, filename ):
    self._conn = sqlite.connect(filename)
    self.commit = self._conn.commit

    if self._SchemeVersion() < 1:
      cur = self._conn.cursor()

      cur.execute("CREATE TABLE version_info (scheme integer)")
      cur.execute("INSERT INTO version_info (scheme) VALUES (1)")

      cur.execute("CREATE TABLE edges (fromNode INTEGER NOT NULL, toNode INTEGER NOT NULL, UNIQUE (fromNode, toNode))")
      cur.execute("CREATE TABLE nodes (nid INTEGER PRIMARY KEY, name VARCHAR(128) NOT NULL, contentPath VARCHAR(512))")
      self._conn.commit()

  def GetNodes( self ):
    """
      Return a generator for all the nodes in the database. Use this like:
      
      for node in db.GetNodes():
        ...

      or:

      nodeList = list(db.GetNodes()) # this could use a lot of memory
    """
    cur = self._conn.cursor()
    cur.execute("SELECT nid FROM nodes")
    for (nid,) in cur:
      yield Node(nid, self)

  def GetLinks( self ):
    """
      Return a generator for all the links (edges) in the database. Use this like:
      
      for (fromNode, toNode) in db.GetEdges():
        ...

      or:

      edgeList = list(db.GetEdges()) # this could use a lot of memory
    """
    cur = self._conn.cursor()
    cur.execute("SELECT fromNode, toNode FROM edges")
    for (fromNode, toNode) in cur:
      yield (Node(fromNode, self), Node(toNode, self))

  def GetNodeById( self, nid ):
    """
    Return a Node object for the node with nid == nid. This will not be the same object when 2 calls are made with the same nid.
    """
    return Node(nid, self)

  def GetNodeByContentPath( self, path ):
    """
    Return a Node object for the first node with contentPath == path.
    """
    cur = self._conn.cursor()
    cur.execute( "SELECT nid FROM nodes WHERE contentPath = ? ORDER BY nid ASC LIMIT 1", (path,) )
    try:
      return Node(cur.fetchone()[0], self)
    except TypeError:
      raise KeyError(name, "not a content path in the DB")

  def GetNodeByName( self, name ):
    """
    Return a Node object for the first node with name == name.
    """
    cur = self._conn.cursor()
    cur.execute( "SELECT nid FROM nodes WHERE name = ? ORDER BY nid ASC LIMIT 1", (name,) )
    try:
      return Node(cur.fetchone()[0], self)
    except TypeError:
      raise KeyError(name, "not a name in the DB")

  def AddNode( self, name, contentPath ):
    """
    Add a node with the given name and content path. Return an assoiated node object.
    """
    cur = self._conn.cursor()
    cur.execute( "INSERT INTO nodes (name, contentPath) VALUES (?, ?)", (name, contentPath) )
    #cur.execute( "SELECT nid FROM nodes WHERE contentPath = ?", (contentPath,) )
    self._conn.commit()

    return Node(cur.lastrowid, self)

  def AddLink( self, fromNode, toNode ):
    """
    Add a link (edge) from fromNode to toNode.
    """
    cur = self._conn.cursor()
    cur.execute( "INSERT INTO edges (fromNode, toNode) VALUES (?, ?)", (fromNode.GetNID(), toNode.GetNID()) )
    self._conn.commit()

  def GetNodeCount( self ):
    cur = self._conn.cursor()
    cur.execute( "SELECT count(nid) FROM nodes" )

    return cur.fetchone()[0]
    

  def _SchemeVersion( self ):
    try:
      cur = self._conn.cursor()
      cur.execute("select * from version_info")
      return cur.fetchone()[0]
    except:
      return 0

