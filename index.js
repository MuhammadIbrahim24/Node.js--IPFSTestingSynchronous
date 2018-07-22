const q1 = 'FILESYSTEM';
const q2 = 'FILESYSTEM_RESPONSE';
const open = require('amqplib').connect('amqp://username:password@url:port');

var i = 0;

// Publisher
function publisher() {
	open.then(function(conn) {
		return conn.createChannel();
	}).then(function(ch) {
		send(ch);
	}).catch(console.warn);
	return;
}

function send(ch) {
	i++;
	if(i > 1000) {
		writer.end();
		return;
	}

	const HEX_CHARS = "0123456789ABCDEF";
	const FILE_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	var key = makeString(HEX_CHARS, 64);
	var file = makeString(FILE_CHARS, Math.floor((Math.random() * 50) + 20));
	
	ch.assertQueue(q1, {durable: false}).then(ch.sendToQueue(q1, new Buffer(JSON.stringify({
				"type":1,
				"file":file,
				"contentType":"text",
				"id":i,
				"key":key
			})))).then(receive(ch));
}



function receive(ch) {
  ch.assertQueue(q2, {durable: false}).then(ch.consume(q2, function(msg) {
	if(msg !== null) {
      console.log(msg.content.toString());
      ch.ack(msg);
	  
	  var obj = JSON.parse(msg.content.toString());
					
	  writer.write({Id: obj.id, Hash: obj.Hash, StartTime: obj.startTime, EndTime: obj.endTime, Difference: obj.endTime-obj.startTime});
	  send(ch);
    }
  }))
}


//generate string
function makeString(charsArr, strLength) {
	const CHARS = charsArr;
	const STRING_LENGTH = strLength;
	var randomstring = '';
	for (var i = 0; i < STRING_LENGTH; i++) {
		var rnum = Math.floor(Math.random() * CHARS.length);
		randomstring += CHARS.substring(rnum,rnum+1);
	}
	return randomstring;
}




//Main program
var fs = require('fs');    //write values to csv file
var csvWriter = require('csv-write-stream');
var writer = csvWriter({sendHeaders: false});
writer.pipe(fs.createWriteStream('1000_requests_sync.csv'));
writer.write({Id: "Id", Hash: "Hash", StartTime: "Start Time", EndTime: "End Time", Difference: "Difference"});

publisher();
