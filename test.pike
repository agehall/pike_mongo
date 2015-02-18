object client;

mapping collections = ([]);

void got_collections(mapping(string:object) collections) {
  this_program::collections = collections;
  run_state = 100;
}

int run_state = 0;
void run_loop() {
  switch(run_state) {
  case 0:
    if (client) {
    } else {
      client = Mongo.Client();
      client->connect("127.0.0.1", 27017,
		      lambda() {
			// fully_connected_cb
			werror("Connected!\n");
			run_state = 1;
		      });
    }
    break;

  case 1: // We have an open connection and can start processing stuff

    object tmp_db = client->get_db("test2");
    object tmp_col = tmp_db->get_collection("test");

    tmp_col->query(([]), lambda(mapping res) {
			   werror("res: %O\n", res);
			 });
#if 0
    tmp_col->insert(([
		      "hej" : "Hopp",
		      "foo" : "Bar",
		    ]));
#endif


    client->list_dbs(lambda(array dbs) {
		       werror("dbs: %O\n", dbs);
		       foreach(dbs;; object db) {
			 if (db->db_name == "test") {
			   db->collections(got_collections);
			 }
		       }
		     });

    break;

  case 100: // We now have our collections and can get started...
    werror("Got the following collections: %O\n", collections);

    if (!collections->zips) {
      werror("Zip code collection not found\n");
      exit(1);
    }

#if 0
    collections->zips->query(([ "name":([ "$regex":"M.*"]) ]), lambda(mixed res) {
				       werror("%O\n", res->documents);
				       run_state++;
				     });
#endif
    run_state++;
    break;

  case 101:

    // 2015-02-10T15:27:43.663+0100 [conn1] run command test.$cmd { aggregate: "zips", pipeline: [ { $group: { _id: "$state", totalPop: { $sum: "$pop" } } }, { $match: { totalPop: { $gte: 10000000.0 } } } ], cursor: {} }
    // 2015-02-10T15:27:43.694+0100 [conn1] command test.$cmd command: aggregate { aggregate: "zips", pipeline: [ { $group: { _id: "$state", totalPop: { $sum: "$pop" } } }, { $match: { totalPop: { $gte: 10000000.0 } } } ], cursor: {} } keyUpdates:0 numYields:1 locks(micros) r:22850 reslen:335 31ms


    // 2015-02-10T15:28:32.072+0100 [conn2] run command test.$cmd { pipeline: [ { $match: { totalPop: { $gte: 10000000 } } }, { $group: { _id: "$state", totalPop: { $sum: "$pop" } } } ], aggregate: "zips" }
    // 2015-02-10T15:28:32.072+0100 [conn2] command test.$cmd command: listDatabases { pipeline: [ { $match: { totalPop: { $gte: 10000000 } } }, { $group: { _id: "$state", totalPop: { $sum: "$pop" } } } ], aggregate: "zips" } ntoreturn:1 keyUpdates:0 numYields:0  reslen:243 0ms

    // { aggregate: "Test", pipeline: [], cursor: {} }
    werror("Requesting aggregate\n");
    collections->zips->aggregate(([
				   // "aggregate" : "zips",
				   "pipeline": ({
				     ([ "$group" : ([ "_id" : "$state", "totalPop" : ([ "$sum" : "$pop" ]) ]) ]),
				     ([ "$match" : ([ "totalPop" : ([ "$gte" : 10*1000*1000 ]) ]) ]),
				   }),
				   // "cursor": ([])
				 ]), lambda(mixed res) {
				       werror("aggregation: %O\n", res->documents);
				       run_state++;
				     });
    break;


  case 102:
    exit(0);


  default:
  }

  call_out(run_loop, 0.1);
}

int main() {
  run_loop();

  return -1;
}
