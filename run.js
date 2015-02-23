//! run.js
//! Homegrown Marine log processor
//! version : 0.0.0
//! homegrownmarine.com


var path = require('path');
var fs = require('fs');
var JSONStream = require('JSONStream');
var CombinedStream = require('combined-stream');
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

function nmeaCollector(oneHz) {
    // this will return a function to collect nmea messages into
    // time keyed states.  Will return undef for messages that
    // don't generate a state
    var now = {};

    return function (message) {
        var data = nmea.parse(message);
        if (!data) return;

        var returnValue;

        
        if (data.time && 
            //if this is a new second, to control at 1Hz
            (!oneHz || data.time.milliseconds() == 200) ) {

            returnValue = now;
            delete returnValue.msg;
            delete returnValue.type;
            delete returnValue.variation;
            delete returnValue.latStr;
            delete returnValue.lonStr;

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

function offsetTimeAppender(referenceTime) {
    return function(sample) {
        sample.t = sample.time.diff(referenceTime) / 1000;
        return sample;
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

function getNMEAStream(beginTime, endTime) {
    //assumes files are returned in order
    var files = filesForTimeRange(beginTime, endTime);
    
    var cs = CombinedStream.create();
    _.each(files, function(file) {
        var source = fs.createReadStream('/race/data/raw/' + file);
        cs.append(source);
    });

    return cs.pipe(es.split('\r\n'));
}

var buildData = require('./extend.js').buildData;

function processRace(race, callback) {
    console.info("processing ", race.id);

    var startTime = moment(race.date+' '+race.startTime, "YYYYMMDD HH:mm");
    // start displaying 5 minutes before the start
    var beginTime = moment(startTime).subtract(5, 'minutes');
    var endTime = moment(race.date+' '+race.endTime, "YYYYMMDD HH:mm");

    var target = fs.createWriteStream('data/races/'+race.id+'.js');

    //if the jsonStringifier isn't ended, it will write multiple copies of whatever 
    //is piped into it.  Instead, set 'close' to empty, then add my own close at the
    //end of all writing.
    var jsonStringifier = JSONStream.stringify('[\n', sep=',\n', close=']');

    var raceData = [];
    
    var nmeaStream = getNMEAStream(beginTime, endTime);

    nmeaStream
        .pipe(es.mapSync(nmeaCollector(true)))
        .pipe(es.mapSync(nmeaTimeFilter(beginTime, endTime)))
        .pipe(es.mapSync(offsetTimeAppender(startTime)))
        .pipe(jsonStringifier)
        .pipe(target);

    nmeaStream.on('end', function() {
        if( typeof callback == 'function') callback();
    });
}

function processRace2(race, callback) {

    var startTime = moment(race.date+' '+race.startTime, "YYYYMMDD HH:mm");
    // start displaying 5 minutes before the start
    var beginTime = moment(startTime).subtract(5, 'minutes');
    var endTime = moment(race.date+' '+race.endTime, "YYYYMMDD HH:mm");

    var nmeaStream = getNMEAStream(beginTime, endTime);
        
    nmeaStream
        .pipe(es.mapSync(nmeaCollector(false)))
        .pipe(es.mapSync(nmeaTimeFilter(beginTime, endTime)))
        .pipe(es.mapSync(offsetTimeAppender(startTime)))
        .pipe(es.writeArray(function (err, data){
            race.data = raceData;
            buildData(race);

            tacks = _.filter(tacks, function(tack) {
                return tack.race_id != race.id;
            });

            var ltacks = _.map(race.tacks, function(tack) { tack.race_id = race.id; return tack; });
            tacks = tacks.concat( ltacks );  

            if( typeof callback == 'function') callback();
        })); 

}

var fileContents = fs.readFileSync('data/races.js'); 
var races = JSON.parse(fileContents);
var tacks = JSON.parse(fs.readFileSync('data/tacks.js')); 

function writeTacks() {
    console.info('writing tacks', tacks);

    fs.writeFileSync( 'data/tacks.js', JSON.stringify(tacks, null, 4) );
}

if ( id ) {
    var race = _.find(races, function(r) { return r.id == id; });
    processRace(race, function() { writeTacks(); });
}
else {
    var processRaces = _.filter(races, function(r) { return r.boat == "Project Mayhem"; });

    async.eachSeries(processRaces, function(race, callback) {
        processRace(race, callback);
    }, function() { writeTacks(); });
}





// process for maneuvers, etc