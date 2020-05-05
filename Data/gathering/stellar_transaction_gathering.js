const Stellar    =   require("stellar-sdk");
const Mongo      =   require("mongodb").MongoClient;
const assert      =   require("assert");
const https      =   require('https');
const fs         =   require('fs');
const serverUrl  =   "mongodb://127.0.0.1:27017/"; // defualt mongo's server address and port number.
const dbName     =   "s_test";
const server     =   new Stellar.Server('https://horizon.stellar.org/');
var   db         =   null;
const ledgerColl = "ledgers";
const transColl  = "operations";
var   ledgersNum = 0;
var   transNum   = 0;

Mongo.connect(serverUrl,function(err, client){
    if(err) throw err;
    assert.equal(null  ,err);
    db = client.db(dbName);
});
requestToStellar();

async function requestToStellar(){
//  var ledgerNumber    = 23194920;//Start at 1/11/1399
//  var ledgerNumber    = 23144642;//12/1/1399
//  var ledgerNumber    = 23089597;//13/1/1399
    // var ledgerNumber    = 22261415;//21/1/1399
    var ledgerNumber    = 23089597;//21/1/1399
  do{
  fs.writeFileSync('currentSequence.txt', ledgerNumber, (err) => {
      // We couldn't give data, so we have to log it.
      if (err) throw err;
    });
    https.get("https://horizon.stellar.org/ledgers/" + ledgerNumber, (resp) => {
      // A chunk of data has been recieved.
      resp.on('data', (chunk) => {
        var data = JSON.parse(chunk);
        var ledger = ledgerMapper(data);// Mapping input data to our structure.
        // writeOnDb(ledgerColl, ledger, db, ledgerSaved, "");// Write our ledger on Mongo db.
          loadTransactions(ledger, false);// Load transactions of current ledger and Write them into Mongo db.
      });

    }).on("error", (err) => {
      let msg =  "1# : " + ledgerNumber +  " .\n -- " + err.message + "\n";
      fs.appendFile('ledger_error.txt', msg, (err) => {
          // We couldn't give data, so we have to log it.
          if (err) throw err;

      });
    });
      await sleep(1000);// we make delay to
    ledgerNumber--;
  }while(ledgerNumber >= 22244619);
}
function ledgerMapper(ledger){
  var date = new Date(ledger.closed_at);// we need ISO date structure.
    return [{
        "_id" :                         ledger.id,
        "sequence":                     ledger.sequence,
        "operation_count" :             ledger.operation_count,
        "successful_transaction_count": ledger.successful_transaction_count,
        "failed_transaction_count":     ledger.failed_transaction_count,
        "max_tx_set_size":              ledger.max_tx_set_size,
        "total_coin":                   ledger.total_coins,
        "close_at":                     date.toISOString(),
        "protocol_version":             ledger.protocol_version
    }];
  }
async function loadTransactions(ledger, delay){
  if(delay)
    await sleep(1000);
    server.operations().forLedger(ledger[0].sequence)
        .call()
        .then(function(transactions){
            let  mappedTransactions = [];
            for(var i=0;i<= transactions.records.length - 1;i++){
                if(transactions.records[i].type == "manage_buy_offer"  || transactions.records[i].type == "manage_offer" ||
                    transactions.records[i].type == "manage_sell_offer"){
                      transactionMapping(transactions.records[i],ledger[0].sequence);
                      // mappedTransactions.push(
                    //     transactionMapping(transactions.records[i],ledger[0].sequence)
                    //     );
                      }
                    }
                    
            //   let tranSavedParamas = {"id": ledger[0]._id , "num": mappedTransactions.length};
            // writeOnDb(transColl, mappedTransactions, db, transactionsSaved, tranSavedParamas);
    }).catch(function(err){
      let msg =  "#3 : " + ledger.sequence +  " .\n -- " + err.message  + "\n";
      fs.appendFile('transaction_error.txt', msg, (err) => {
          // throws an error, you could also catch it here
          if (err) throw err;
      });
    });
    await sleep(1000);// we make delay to
}
function transactionMapping(transaction, ledgerId){
  var date = new Date(transaction.created_at);// we need ISO date structure.
    var new_structure =  {
                          "_id":                      transaction.id,
                          "ledger_id":                ledgerId,
                          "transaction_successful":   transaction.transaction_successful,
                          "source_account":           transaction.source_account,
                          "type":                     transaction.type,
                          "type_i":                   transaction.type_i,
                          "created_at":               date.toISOString(),
                          "transaction_hash":         transaction.transaction_hash,
                          "amount":                   parseFloat(transaction.amount),
                          "price":                    parseFloat(transaction.price),
                          "price_r":                  {"n":parseFloat(transaction.price_r["n"]),"d":parseFloat(transaction.price_r["d"])},
                          "buying_asset_type":        transaction.buying_asset_type,
                          "selling_asset_type":       transaction.selling_asset_type
                };
    if(transaction["buying_asset_type"] != "native"){
      new_structure["buying_asset_code"] = transaction.buying_asset_code;
      new_structure["buying_asset_issuer"] = transaction.buying_asset_issuer;
    }
    if(transaction["selling_asset_type"] != "native"){
      new_structure["selling_asset_code"] = transaction.selling_asset_code;
      new_structure["selling_asset_issuer"] = transaction.selling_asset_issuer;
    }
    new_structure["offer_id"] = transaction.offer_id;
    return new_structure;
}
function writeOnDb(collectionName, data, db, callBack, callBackParams){
    var collection = db.collection(collectionName);
    let msg = "";
    let fileName = ".txt";
    collection.insertMany(data,function(err, result){
      if(err){
        if(collectionName == ledgerColl){
          msg =  "#2 : " + data[0].sequence +  " -- " + err.message  + "\n";
          fileName = "ledger_error" + fileName;
        }else if(collectionName == transColl){
          msg =  "#4 : " + data[0].ledger_id +  " -- " + err.message  + "\n";
          fileName = "transaction_error" + fileName;
        }
        fs.appendFile(fileName, msg, (err) => {
          // throws an error, you could also catch it here
          if (err) throw err;
        });
      }
        callBack(result, callBackParams);
    });
}
function ledgerSaved(impResponse, params){
    ledgersNum = ledgersNum + 1;
    console.log(" The number of ledgers have imported :" + ledgersNum);
}
function transactionsSaved(impResponse, params){
    console.log("Ledger " + params.id + " had " + params.num + " operators");
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
