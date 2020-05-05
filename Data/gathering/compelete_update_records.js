
var   MongoClient       = require("mongodb").MongoClient;
const https      =   require('https');
const fs         =   require('fs');
var assert = require('assert');
const SERVER        = "mongodb://127.0.0.1:27017/";
var operations;
var u_operations = null;
var bulk;
var NumberOfUpdated = 0;
var cursor = null;
var dbName = "s_test";

MongoClient.connect(SERVER,{ "useUnifiedTopology": true }, function(err, client){
    if(err) throw err;
    assert.equal(null, err);
    var db = client.db(dbName);
    operations = db.collection("operations") ;
    u_operations = db.collection("u_operations");
    cursor = u_operations.find({},{ "timeout": false});
    console.log(cursor.count() + " operations exists.");
    bulk = [];
    getNext();
});

async function process_one_operation(err, operation){
    if(err){
        console.log(err);
        await getNext();
    }else if(operation === null){
        console.log("Updating compeleted.");
    }else{
        await delete_records({"_id": operation["_id"]});
        getNext();
    }
}


async function delete_records(record){
    // console.log(record);
    await operations.deleteMany(record, 
                async function (err, op) {
                        if (err){ 
                                let msg =  "1# : " + record._id +  " .\n -- " + err.message + "\n";
                                fs.appendFile('delete_data_from_mongo.txt', msg, (err) => {
                                    if (err) throw err;
                                });
                                await sleep(500);
                            }
                  });
    NumberOfUpdated += 1;
    console.log(NumberOfUpdated + " operations have deleted yet.");

}


function getNext(){
    cursor.next(process_one_operation);
}

function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
  
  