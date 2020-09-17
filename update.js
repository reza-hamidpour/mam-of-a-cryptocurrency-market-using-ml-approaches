var myfun = function(data){
            var new_date_time = new Date(data.created_at).toISOString();
            db.E_B_bucket.update({_id: data._id},{$set: {"created_at" : new_date_time}});
}
db.E_B_bucket.find().snapshot().forEach(myfun);


[
 {"$match":
    {"$or":
        [
            {"selling_asset_code": {"$ne": "BTC"}},
            {"selling_asset_code": {"$ne": "ETH"}}
        ]
    }
 },
 {
    $sort:{"created_at":-1}
 }
]

