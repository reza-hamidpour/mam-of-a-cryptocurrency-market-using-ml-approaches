var   MongoClient       = require("mongodb").MongoClient;
const https      =   require('https');
const fs         =   require('fs');
var assert = require('assert');
const SERVER        = "mongodb://127.0.0.1:27017/";
var operations = null;
var u_operations = null;
var bulk;
var NumberOfUpdated = 0;
var cursor = null;
var http_errors;
var mongo_erros;

MongoClient.connect(SERVER, function(err, client){
    if(err) throw err;
    assert.equal(null, err);
    var db = client.db("s_test");
    operations = db.collection("operations") ;
    u_operations = db.collection("u_operations");
    http_errors = fs.createWriteStream("prepare_data.txt", {flags:'a'});
    mongo_erros = fs.createWriteStream("prepare_data_mongo.txt", {flags:'a'});
    cursor = operations.find({"buying_asset_type": {"$ne": "native"},
                            "buying_asset_code": {"$exists": false},
                            "buying_asset_issuer": {"$exists": false}},{ "timeout": false});
    bulk = [];1
    getNext();
});


function process_one_operation(err, operation){
    if(err){
        console.log(err);
        getNext();
    }else if(operation === null){
        console.log("Updating compeleted.");
        http_errors.end();
        mongo_erros.end();
    }else{
        process_operation(operation);
    }
}


async function process_operation(operation){
    await set_oreginal_operation(operation);    
    getNext();
}

function set_oreginal_operation(op){
    https.get("https://horizon.stellar.org/operations/" + op._id, (resp) => {
      resp.on('data', async function(chunk){
        var data = JSON.parse(chunk);
        var new_record =  await getted_operation(op, data);
        bulk.push(new_record);        
        // console.log(" Bulk length :" + bulk.length);
        if(bulk.length == 5){
            await insert_records(bulk);     
            await sleep(4000);
        }
      });
    }).on("error", (err) => {
      
    });
}

function end_of_operation(){
    console.log("Updating completed sucsessfully.");
}

async function insert_records(operations){
    await u_operations.insertMany(operations, 
                async function (err, op) {
                        if (err){ 
                                let msg =  "1# : " + operations._id +  " .\n -- " + err.message + "\n";
                                // mongo_erros.write(msg);
                                // await sleep(500);
                            }
                  });
    bulk = [];
    NumberOfUpdated += operations.length;
    console.log(NumberOfUpdated + " operations have updated yet.");

}


function getNext(){
    cursor.next(process_one_operation);
}

async function getted_operation(l_operation, o_operation){
    var date = new Date(o_operation.created_at);// we need ISO date structure.
      var new_structure = {
                          "_id":                      l_operation._id,
                          "ledger_id":                l_operation.ledger_id,
                          "transaction_successful":   o_operation.transaction_successful,
                          "source_account":           o_operation.source_account,
                          "type":                     o_operation.type,
                          "type_i":                   o_operation.type_i,
                          "created_at":               date.toISOString(),
                          "transaction_hash":         o_operation.transaction_hash,
                          "amount":                   parseFloat(o_operation.amount),
                          "price":                    parseFloat(o_operation.price),
                          "price_r":                  {"n":parseFloat(o_operation.price_r["n"]),"d":parseFloat(o_operation.price_r["d"])},
                          "buying_asset_type":        o_operation.buying_asset_type,
                          "buying_asset_code":        o_operation.buying_asset_code,
                          "buying_asset_issuer":      o_operation.buying_asset_issuer,
                          "selling_asset_type":       o_operation.selling_asset_type,
                };
        if(new_structure["selling_asset_type"] != "native"){
            new_structure["selling_asset_code"] = o_operation.selling_asset_code;
            new_structure["selling_asset_issuer"] =  o_operation.selling_asset_issuer;
        }
        new_structure["offer_id"] = o_operation.offer_id;
      return new_structure;
  }

function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
  
  