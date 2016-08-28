var tables = {};

module.exports.createTable = function(name, keys) {
    var table = {
        HashKey: keys[0],
        Keys: keys,
        Items: {},
        PredefinedQueries: {},
        getKey: function(item) {
            //console.log ("Hash: " + item[this.HashKey]);
            var key = item[this.HashKey];
            if (this.RangeKey) {
                key += "//" + item[this.RangeKey];
                //console.log ("RangeKey: " + JSON.stringify(item[this.RangeKey]));
            }
            return key;
        },
        put: function(item) {
            var key = this.getKey(item);
            this.Items[key] = item;
        },
        get: function(keyObj) {
            var key = this.getKey(keyObj);
            var item = this.Items[key];
            return item;
        },
        delete: function(keyObj) {
            var key = this.getKey(keyObj);
            delete this.Items[key];
        },
        query: function(params) {
            if (params.IndexName) {
                var f = this.PredefinedQueries[params.IndexName];
                if (f)
                    return f(params);
                throw Error("You tried to query index " + params.IndexName + " on table " + params.TableName + " and that requires you first set up the query with setupQuery.\r\nParams were: \r\n" + JSON.stringify(params));
            }
            var filterPairs = params.KeyConditionExpression.split(' AND ');
            var hashFilter, rangeFilter;
            for (var i in filterPairs) {
                var parts = filterPairs[i].split('=');
                if (parts[0].trim() == this.HashKey) {
                    hashFilter = params.ExpressionAttributeValues[parts[1].trim()];
                }
            }
            var matchedItems = [];
            for (var i in this.Items) {
                if (this.Items[i][this.HashKey] == hashFilter)
                    matchedItems.push(this.Items[i]);
            }
            return {
                Count: matchedItems.length,
                Items: matchedItems
            };
        },
        name: name,
        clearData: function() {
            this.Items = {};
        }
    };

    if (keys.length == 2)
        table.RangeKey = keys[1];

    tables[name] = table;
};
module.exports.getItem = function(params, cb) {
    get(params, cb);
};
module.exports.get = function(params, cb) {
    var item = module.exports.synchronousGet(params);
    if (!item) {
        item = null;
    }
    cb(null, {
        Item: item
    });
};
function internalGetTable(params) {
    var table = tables[params.TableName];
    if ( table == null )
        throw Error("Table " + params.TableName + " does not exist");
    return table;
}
module.exports.synchronousGet = function(params) {
    var table = internalGetTable(params);
    return table.get(params.Key);
}
module.exports.putItem = function(params, cb) {
    put(params, cb);
};
module.exports.put = function(params, cb) {
    module.exports.synchronousPut(params);
    cb(null);
};
module.exports.delete = function(params, cb) {
     var table = internalGetTable(params);
    table.delete(params.Key);
    cb(null);
};
module.exports.printDb = function() {
    console.log("TABLES");
    console.dir(tables);
};
module.exports.synchronousPut = function(params) {
    var table = internalGetTable(params);
    table.put(params.Item);
};
module.exports.setupQuery = function(params, func) {
    var table = internalGetTable(params);
    if (!params.IndexName)
        throw Error("You must provide an index to setup a query");
    table.PredefinedQueries[params.IndexName] = func;
};
module.exports.query = function(params, cb) {
    var table = internalGetTable(params);
    cb(null, table.query(params));
}
module.exports.reset = function() {
    for (var i in tables) {
        tables[i].clearData();
    }
};
module.exports.clearTable = function(name) {
    tables[name].clearData();
}