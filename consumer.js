var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;

var fs= require('fs')

var client = new Client('localhost:2181');
var topics = [{
  topic: 'node-test'
}];



var options = {
  autoCommit: true,
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024,
  encoding: 'buffer'
};
var consumer = new HighLevelConsumer(client, topics, options);

consumer.on('message', function(message) {
  var buf = new Buffer(message.value, 'binary'); // Read string into a buffer.
 // var decodedMessage = type.fromBuffer(buf.slice(0)); // Skip prefix.
  console.log(buf);

    fs.writeFile("test.txt", buf,function(err) {
      console.log('data written ')
   });  

});

consumer.on('error', function(err) {
  console.log('error', err);
});

process.on('SIGINT', function() {
  consumer.close(true, function() {
    process.exit();
  });

});


