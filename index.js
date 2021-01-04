// This is sample a TairTS data source for Grafana.
// Use SimpleJSON data source to connect to this server.

var express = require('express');
var bodyParser = require('body-parser');
var redis = require('redis');
var _ = require('lodash');
var app = express();

var redisPort = process.argv[3];
var reidsHost = process.argv[2];
var redisClient = redis.createClient(redisPort, reidsHost);

// Scan 1000 keys at a time
var SCAN_START = 0;
var SCAN_COUNT = 1000;

// TairTS data type
var TYPE_TIMESERIES = 'exts-type';


app.use(bodyParser.json());

function setCORSHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST");
  res.setHeader("Access-Control-Allow-Headers", "accept, content-type");
}


// Runs "key.type" function in batches. scanBatch scans 1000 keys
// at a time and passes those keys to this function. runBatch
// pipelines the calls into one batch, instead of making 1000 calls.
function runBatch(req, res, keys, nextCursor, count, endFlag, tsList){
  batch = redisClient.batch();

  _.each(keys, function(val){
    batch.type(val);
  });

  // Batch call to Redis
  batch.exec( function(err, results){
    if(err){
      console.log("Error"+err);
      return;
    }else{
      var c = 0;
      _.each(results, function(val){
        if(val == TYPE_TIMESERIES){
          tsList.push(keys[c]);
        }
        c++;
      });
      scanBatch(req, res, nextCursor, SCAN_COUNT, endFlag, tsList);
    }
  });
}

// Scans all the keys of type TSDB-TYPE as we are interested only
// in time series data
function scanBatch(req, res, cursor, count, endFlag, tsList){
  if(endFlag){
    //console.log("End cursor");
    //console.log("TS LIST: "+tsList);
    res.json(tsList);
    res.end();
    return;
  }

  // Redis call
  redisClient.send_command('SCAN', [cursor, 'count', count], function (err, results){
    if(err){
      console.log("Error"+err);
    }else{
      endFlag = false;
      if(results[0] == '0'){
        endFlag = true;
      }
      runBatch(req, res, results[1], results[0], SCAN_COUNT, endFlag, tsList);
    }
  });
}


// This function makes a EXTS.P.RANGE call to the timeseries database
function runTimeSeriesCommand(req, res, from, to, TimeBucket, targets, index, responseArray){

    if(targets == null || targets.length <= index){
      console.log("EXIT index:"+index);
      res.json(responseArray);
      res.end();
    }else{

      target = targets[index].target;
      newIndex = index + 1;
      redisClient.send_command('EXTS.P.RANGE', [target, from, to, 'avg', TimeBucket, 'AGGREGATION', 'sum', TimeBucket, 'FILTER', process.argv[4]], function(err, value){
         if(err){
            console.log("Error:"+err);
         }

        var datapoints = [];
         _.each(value, function(val){
             if(val[0] && val[1]){
                 datapoints.push([val[1], val[0]]);
             }
         });
         console.log(datapoints);
         responseArray.push({target, datapoints});

         runTimeSeriesCommand(req, res, from, to, TimeBucket, targets, newIndex, responseArray);
      });
    }
}

//--------------------------------------------------
// Grafana SimpleJson adapter calls start here
//--------------------------------------------------

// HTTP query 1: "/"
app.all('/', function(req, res) {
  setCORSHeaders(res);
  res.send('I have a quest for you!');
  res.end();
});


// HTTP query 2: "/search"
// Search all keys of type TSDB-TYPE
// This command uses the scan command instead of
// the keys command.
app.all('/search', function(req, res){
  setCORSHeaders(res);
  var tsList = [];
  scanBatch(req, res, SCAN_START, SCAN_COUNT, false, tsList)
});


// HTTP query 3: "/query"
// Translates JSON query to TS query
app.all('/query', function(req, res){
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  from = new Date(req.body.range.from).getTime();
  to = new Date(req.body.range.to).getTime();
  TimeBucket = req.body.intervalMs;
  targets = req.body.targets;

  var responseArray = [];
  index = 0;
  runTimeSeriesCommand(req, res, from, to, TimeBucket, targets, index, responseArray);

});


// HTTP query 4: "/annotations"
// Not implemented for now.
app.all('/annotations', function(req, res) {
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  res.json(annotations);
  res.end();
});



app.listen(3333);

console.log("Server is listening to port 3333");
