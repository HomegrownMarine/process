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
var program = require('commander');

program
    .version('0.0.1')
    .parse(process.argv);

var id = program.args[0];



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

        //if this is a new second, to control at 1Hz
        if (data.time && data.time.milliseconds() == 200) {

            data.t = data.time.diff(startTime) / 1000;

            returnValue = now;
            delete returnValue.msg;
            delete returnValue.type;
            delete returnValue.variation;
            delete returnValue.latStr;
            delete returnValue.lonStr;
            // delete returnValue.time;

            now = {};
        }

        now = _.extend(now, data);

        return returnValue;
    };
}

function nmeaTimeFilter(startTime, endTime) {
    //filter datapoints by time
    return function(sample) {
        if ( startTime <= sample.time && sample.time <= endTime ) {
            return sample;                
        }
    };
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


function processRace(race, callback) {
    console.info("processing ", race.id);

    var startTime = moment(race.date+' '+race.startTime, "YYYYMMDD HH:mm");
    var beginTime = moment(startTime).subtract(5, 'minutes');
    var endTime = moment(race.date+' '+race.endTime, "YYYYMMDD HH:mm");

    var files = filesForTimeRange(startTime, endTime);
    var target = fs.createWriteStream('races/'+race.id+'.js');

    //if the jsonStringifier isn't ended, it will write multiple copies of whatever 
    //is piped into it.  Instead, set 'close' to empty, then add my own close at the
    //end of all writing.
    var jsonStringifier = JSONStream.stringify('[\n', sep=',\n', close='');

    async.eachSeries( files, function(file, callback) {
        var source = fs.createReadStream('/race/data/raw/' + file);
        source
            .pipe(es.split('\r\n'))
            .pipe(es.mapSync(nmeaCollector(startTime)))
            .pipe(es.mapSync(nmeaTimeFilter(beginTime, endTime)))
            .pipe(jsonStringifier)
            .pipe(target, {end: false}); 

        source.once('end', function() {
            callback();
        }); 
    }, function() {
        target.end('\n];\n');
        if ( typeof callback == "function" ) callback();
    });
}

var fileContents = fs.readFileSync('races.js', 'utf8'); 
var races = JSON.parse(fileContents);

if ( id ) {
    var race = _.find(races, function(r) { return r.id == id; });
    processRace(race);    
}
else {
    var processRaces = _(races)
        .filter(function(r) { return r.boat == "Project Mayhem"; })
        .valueOf();

    async.eachSeries(processRaces, function(race, callback) {
        processRace(race, callback);
    });
}



// process for maneuvers, etc