// .WireProtocol wire;

object MyBSON = class {
  inherit Standards.BSON.module;

  string encode(array(mapping)|mapping m, int|void query_mode) {
    String.Buffer buf = String.Buffer();

    if (arrayp(m)) {
      foreach(m, mapping tmp) {
	low_encode(tmp, buf, query_mode);
      }
    } else {
      low_encode(m, buf, query_mode);
    }

    return sprintf("%-4c%s%c", sizeof(buf)+5, buf->get(), 0);
  }
  }();

class MongoCommand {
  constant command = "";

  object my_db;

  static void create(object db) {
    werror("Creating mogo command %O\n", command);
    my_db = db;
  }

  void db_cb(.WireProtocol.MongoReply r, function cb, mixed ... args) {
    werror("db_cb: %O\n", r && r->documents);
    werror("cb: %O\n", cb);
    if (functionp(cb)) {
      cb(r, @(args||({})));
    }
  }

  void exec(Database admin_db, function cb, mixed ... args) {
    werror("Exec in command %O\n", command);
    admin_db->send_command(.WireProtocol.OP_QUERY, ([ command : 1]), db_cb, cb, @args);
  }

  void `()(function cb, mixed ... args) {
    werror("Executing mongo command %O\n", command);
    if (!my_db) my_db = admin_db;
    exec(my_db, cb, @(args||({})));
  }
}

class MongoQuery {
  constant command = "Query";
  string|object col;

  static void create(string|object col) {
    this_program::col = col;
  }


  void db_cb(.WireProtocol.MongoReply r, void|function cb, void|mixed ... args) {
    // werror("db_cb: %O\n", r && r->documents);
    if (functionp(cb)) {
      cb(r, @(args||({})));
    }
  }

  void exec(.WireProtocol wire, mapping query, void|function(mapping:void) cb, void|mixed ... args) {
    string collection, db_name;

    if (stringp(col)) {
      db_name = "admin";
      collection = col;
    } else {
      collection = col->col_name;
      db_name = col->db->db_name;
    }

    werror("%O %O\n", col, col->db);
    wire->send_query(db_name, collection, 4, query, db_cb, cb, @(args||({})));
  }

  void `()(mapping query, void|function(mapping:void) cb, void|mixed ... args) {
    object wire = get_a_wire();
    exec(wire, query, cb, @(args||({})));
  }
}

class Database {
  class Collection {

    string col_name;
    object db;

    static void create(object db, string col_name) {
      this_program::col_name = col_name;
      this_program::db = db;
    }

    object insert = class {
	inherit MongoCommand;
	constant command = "insert";

	void exec(.WireProtocol wire, array(mapping)|mapping documents,
		  void|function(mapping:void) cb, void|mixed ... args) {
	  if (mappingp(documents)) {
	    documents = ({ documents });
	  }

	  db->send_command(.WireProtocol.OP_INSERT, ({
			     ([ "insert" : col_name ]),
			     ([ "documents" : documents ]),
			   }),
			   db_cb, cb, @(args||({})));
	}
      }(this);

    object query = class {
	inherit MongoQuery;
      }(this);

    object aggregate = class {
	inherit MongoCommand;
	constant command="aggregate";

	static void create(object parent) {
	  ::create(parent);
	}

	void exec(.WireProtocol wire, mapping query, function(mapping:void) cb, mixed ... args) {
	  query->aggregate = col_name;
	  // query->pipeline = query->pipeline || ({});
	  // query->cursor   = query->cursor || ([]);

	  db->send_command(.WireProtocol.OP_QUERY, ({
			     ([ "aggregate" : col_name ]),
			     query
			   }), db_cb, cb, @(args||({})));
	}

	void `()(mapping agg, function cb, mixed ... args) {
	  werror("Executing aggregation query: %O %O\n", agg, args);
	  if (!my_db) my_db = admin_db;
	  exec(my_db, agg, cb, @(args||({})));
	}

      }(this);
  }


  string db_name;
  this_program this_pointer;

  static void create(string db_name) {
    this_pointer = this;
    this_program::db_name = db_name - ".";
  }


  static string _sprintf(int type) {
    return sprintf("Database(%s)", db_name);
  }

  void send_command(int opcode,
		    array|mapping command,
		    function cb,
		    mixed ... args) {
    werror("db send command %O %O\n", command, args);
    string data = sprintf("%-4c%s.%s\0%-4c%-4c%s",
			  4, db_name, "$cmd", 0, 1,
			  MyBSON.encode(command,1));
    object w = indices(connected_wires)[0];
    w->send_request(opcode,
		    data, cb, @(args||({})));
  }

  mapping(string:Collection) my_collections = ([]);
  Collection get_collection(string name) {
    my_collections[name] = my_collections[name] || Collection(this_pointer, name);
    return my_collections[name];
  }

  function collections = class {
      inherit MongoQuery;

      void db_cb(object res, function cb, mixed ... args) {
	mapping cols = set_weak_flag(([]), Pike.WEAK_VALUES);

	foreach(res->documents;; mapping col_info) {
	  string col_name = col_info->name;

	  // Make sure this is a collection we should create an object for...
	  if (!has_prefix(col_name, db_name)) continue;
	  if (search(col_name, "$")!=-1) continue;

	  col_name = col_name[sizeof(db_name)+1..]; // Strip db name and initial "."
	  if (has_prefix(col_name, "system")) continue;

	  // Use the existing one if we have it.
	  cols[col_name] = get_collection(col_name);
	}

	my_collections = cols;

	if (functionp(cb)) {
	  cb(my_collections, @(args||({})));
	}
      }

      void exec(.WireProtocol wire, function(mapping:void) cb, mixed ... args) {
	wire->send_query(db_name,
			 stringp(col)?col:col->col_name,
			 4,
			 ([]),
			 db_cb, cb, @(args||({})));
      }

    }("system.namespaces");
}

Database admin_db;

mapping(string:object) mongos = ([]);

static void create() {
  admin_db = Database("admin");
  dbs->admin = admin_db;
}

// FIXME: Need weak mappings?
multiset(.WireProtocol) connected_wires = (<>);
mapping(string:multiset(.WireProtocol)) wires_by_url = ([]);
string primary_url;

// Return a random wire connection
.WireProtocol get_a_wire() {
  return random(connected_wires);
}

void connect(void|string host, void|int port, function fully_connected_cb) {
  void is_master_cb(mapping doc) {
    // FIXME: Handle all the following cases:
    // [ ] Stand alone
    // [X] Replica Set
    // [ ] Shard


    array hosts = doc->hosts;
    if (hosts) {
      foreach(hosts;; string host) {
	if (!wires_by_url[host])
	  wires_by_url[host] = (<>);
      }

      primary_url = doc->primary;
    } else if (doc->ismaster) {
      // No hosts in reply and ismaster => we are talking to a stand
      // alone instance.

      primary_url = sprintf("%s:%d",
			    host||"localhost",
			    27017);
    }

    if (!sizeof(wires_by_url[primary_url])) {
      sscanf(primary_url, "%s:%d", string host, int port);
      object w = .WireProtocol(host, port);
      w->connect(lambda(int res, object w) {
		   if (sizeof(wires_by_url)) {
		     destruct(w);
		   } else {
		     wires_by_url[primary_url][w] = 1;
		   }
		 });
    }

    // We now know our primary URL and all hosts in the replica set.
    fully_connected_cb();
  };

  void connect_cb(int res, object wire) {
    // We now need to list all replicas / shards and only when we know
    // about these can we consider ourselves fully connected.

    if (!res) {
      // FIXME: Proper error handling...
      werror("Connection error\n");
      exit(1);
    }

    werror("Inital connection setup\n");
    connected_wires[wire] = 1;
    wires_by_url[sprintf("%s:%d", wire->remote_host, wire->remote_port)] = (< wire >);

    isMaster(is_master_cb);
  };

  host = host || "localhost";
  port = port || 27017;

  object wire = .WireProtocol(host, port);
  wire->connect(connect_cb);
}


mapping dbs = ([]);

Database get_db(string name) {
  dbs[name] = dbs[name] || Database(name);
  return dbs[name];
}

function list_dbs = class {
    inherit MongoCommand;
    constant command = "listDatabases";

    void db_cb(.WireProtocol.MongoReply r, function cb, mixed ... args) {
      array res = ({});
      foreach(r->documents[0]->databases; int i; mapping db) {
	dbs[db->name] = dbs[db->name] || Database(db->name);
	res += ({ dbs[db->name] });
      }

      cb(res, @(args||({})));
    };

  }(admin_db);

object isMaster = class {
    inherit MongoCommand;
    constant command = "isMaster";

    void db_cb(.WireProtocol.MongoReply r, function cb, mixed ... args) {
      // werror("%O\n", r->documents);
      // XXX: functionp(cb) returns false!?!
      (cb) && cb(r->documents[0], @(args||({})));
    }
  }(admin_db);
