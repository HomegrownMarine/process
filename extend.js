var _ = require('lodash');
var moment = require('moment');

var homegrown = require('homegrown-sailing');

function buildData(race) {
    
    var calcs = homegrown.calcs;
    var delayedInputs = homegrown.utilities.delayedInputs;
    var derivitive = homegrown.utilities.derivitive;
    var average = homegrown.utilities.average;
    
    var NM_TO_FT = 6076.11549;
    var awa_offset = 5;

    var xforms = [
        function calibrate(args) {
            if ( 'awa' in args ) {
                args.awa -= awa_offset;
                if (args.awa > 180) {
                    args.awa = -1 * (360 - args.awa);
                }
            }
        },

        delayedInputs(calcs.tws),
        delayedInputs(calcs.twa),
        delayedInputs(calcs.twd),
        delayedInputs(calcs.gws),
        delayedInputs(calcs.gwd),
        delayedInputs(calcs.set),
        delayedInputs(calcs.drift),
        delayedInputs(calcs.vmg),

        derivitive('acceleration', 'speed', (NM_TO_FT / 3600)),
        derivitive('rot', 'hdg')
    ];

    
    var dat = race.data;
    var offset = moment(race.date+' '+race.startTime, "YYYYMMDD HH:mm").valueOf();

    for ( var i=0; i < dat.length; i++ ) {
        var pt = dat[i];
        if ('t' in pt) {
            pt.ot = pt.t;
            pt.t = pt.ot*1000 + offset;
        }

        // testing calibration approaches here
        if ( 'hdg' in pt) {
            pt.hdg = pt.hdg + 0;
        }

        if ( 'speed' in pt ) {
            pt.speed = pt.speed * 1.05;
        }
    
        for ( var x=0; x < xforms.length; x++ ) {
            var xform = xforms[x];

            var result = xform(pt);
            if (result) {
                for (var k in result) {
                    pt[k] = result[k];
                }
            }
        }
    }
    
    race.maneuvers = homegrown.maneuvers.findManeuvers(dat);
    race.tacks = homegrown.maneuvers.analyzeTacks(race.maneuvers, dat);
}

exports.buildData = buildData;