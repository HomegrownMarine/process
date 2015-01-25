//! run.js
//! Homegrown Marine log processor
//! version : 0.0.0
//! homegrownmarine.com


var path = require('path');
var fs = require('fs');
var JSONStream = require('JSONStream');
var es = require('event-stream');

var winston = require('winston');
var _ = require('lodash');
var moment = require('moment');
var async = require('async');

var nmea = require('homegrown-nmea');


//arguments
// var program = require('commander');

// program
//     .version('0.0.1')
//     // .option('-b, --bbq', 'Add bbq sauce')
//     // .option('-c, --cheese [type]', 'Add the specified type of cheese [marble]', 'marble')
//     .parse(process.argv);


//get times for race
//get files for time

//parse files
// -- for each point, collapse to nmea entry, 
// -- filter by time
// -- write as json


function nmeaCollector(startTime) {
    // this will return a function to collect nmea messages into
    // time keyed states.  Will return undef for messages that
    // don't generate a state
    var now = {};

    return function (message) {
        var data = nmea.parse(message);
        if (!data) return;

        var returnValue;

        if (data.time) {
            data.t = data.time.diff(startTime) / 1000;
            
            returnValue = now;
            delete returnValue.msg;
            delete returnValue.type;

            now = {};
        }

        now = _.extend(now, data);

        return returnValue;
    }
}

function nmeaTimeFilter(startTime, endTime) {
    //filter datapoints by time
    return function(sample) {
        if ( startTime <= sample.time && sample.time <= endTime ) {
            return sample;                
        }
    }
}

function filesForTimeRange(startTime, endTime) {
    var files = [];
    var hour = moment(startTime);
    var until = moment(endTime).endOf('hour');

    while (hour < until) {
        files.push( hour.format("YYMMDDHH.txt") );
        hour.add(1, 'hour');
    }

    return files;
}


var startTime = moment('20141213'+' '+'11:15', "YYYYMMDD HH:mm").subtract(5, 'minutes');
var endTime = moment('20141213'+' '+'13:18', "YYYYMMDD HH:mm");

var files = filesForTimeRange(startTime, endTime);
var target = fs.createWriteStream('snowbird_1.json');

async.eachSeries( files, function(file, callback) {
    var source = fs.createReadStream('/race/data/raw/' + file);
    var jsonStringifier = JSONStream.stringify('[\n', sep=',\n', close='\n]');

    source
        .pipe(es.split('\r\n'))
        .pipe(es.mapSync(nmeaCollector()))
        .pipe(es.mapSync(nmeaTimeFilter(startTime, endTime)))
        .pipe(jsonStringifier)
        .pipe(target, {end: false}); 

    source.once('end', function() {
        callback();
    }); 
});





// process?