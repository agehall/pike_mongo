enum OP_CODES {
  OP_REPLY        = 1,
  OP_MSG          = 1000,
  OP_UPDATE       = 2001,
  OP_INSERT       = 2002,
  OP_QUERY        = 2004,
  OP_GET_MORE     = 2005,
  OP_DELETE       = 2006,
  OP_KILL_CURSORS = 2007
}

class MongoHeader {
  int msgLen;
  int reqId;
  int respTo;
  int opCode;

  string encode() {
    return sprintf("%-4c%-4c%-4c%-4c",
		   msgLen, reqId, respTo, opCode);
  }

  int _sizeof() { return 4 * 4; };
}

class MongoReply {
  inherit MongoHeader;

  int responseFlags;
  int cursorId;
  int startingFrom;
  int numberReturned;

  array documents;

  static string _sprintf(int type) {
    switch(type) {
    case 'O':
      return sprintf("MongoReply(%d docs) cId=%d start@%d numRet=%d flags=%04x",
		     sizeof(documents||({})), cursorId, startingFrom, numberReturned, responseFlags);
    default:
      return "MongoReply()";
    }
  }


  static void create(string data) {
    //            hhhhhhhhhhhhhhhhFlagCursStar
    sscanf(data, "%-4c%-4c%-4c%-4c%-4c%-8c%-4c%-4c%s",
	   msgLen, reqId, respTo, opCode,
	   responseFlags, cursorId, startingFrom, numberReturned, data);
    werror("Found %d matches, returning from %d\n", numberReturned, startingFrom);
    werror("%O\n", this);
    if (data && sizeof(data)) {
      documents = Standards.BSON.decode_array(data);
    }
  }
}

string remote_host = "127.0.0.1";
int remote_port = 27017;

object connection;

static void create(string host, int port) {
  connection = Stdio.File();
  remote_host = host;
  remote_port = port;
}



void connect(function cb, mixed ... args) {
  void connect_cb(int res, function cb, mixed ... args) {
    connection->set_read_callback(wire_read_cb);
    functionp(cb) && cb(res, this, @(args||({})));
  };

  connection->async_connect(remote_host, remote_port, connect_cb, cb, @(args||({})));

  /*
  int res = connection->connect(remote_host, remote_port);
  return res;
  */
}

int wire_read_cb(mixed _, string data) {
  // werror("wire_read_cb(%d)\n", sizeof(data));
  object r = MongoReply(data);

  if (outstanding_requests[r->respTo]) {
    mapping or = m_delete(outstanding_requests, r->respTo);
    if (functionp(or->cb)) {
      or->cb(r, @(or->args||({})));
    }
  }

  return 0;
}


mapping outstanding_requests = ([]);

protected int reqId = 1;
int send_request(int opcode, string data, void|function(object, mixed ...:void) cb, void|mixed ... args) {
  object h = MongoHeader();
  h->reqId = reqId++;
  h->opCode = opcode;

  h->msgLen = sizeof(data) + sizeof(h);
  data = h->encode() + data;

  outstanding_requests[h->reqId] = ([
    "cb"   : cb,
    "args" : args,
  ]);

  int cnt = connection->write(data);
  // werror("Wrote %d bytes to wire\n", cnt);
  return h->reqId;
}

void send_query(string db, string collection,
		int flags,
		array|mapping payload,
		function cb, mixed ... args) {
  string data = sprintf("%-4c%s.%s\0%-4c%-4c",
			flags, db, collection, 0, 0);

  werror("payload: %O\n", payload);

  if (mappingp(payload)) {
    data += Standards.BSON.encode(payload,1);
  } else {
    data += Standards.BSON.encode_array(payload);
  }
  send_request(OP_QUERY, data, cb, @(args||({})));
}
