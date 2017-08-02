var kafka = require('kafka-node');
var fs = require('fs');
var HighLevelProducer = kafka.HighLevelProducer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;

var client = new Client('10.51.235.65:2181', 'my-client-id', {
  sessionTimeout: 300,
  spinDelay: 100,
  retries: 2
});

// For this demo we just log client errors to the console.
client.on('error', function(error) {
  console.error(error);
});

var producer = new HighLevelProducer(client);

producer.on('ready', function() {
  // Create message
  fs.readFile("mychannel.tx", function(err, data) {
      console.log('data is ',data)
      var messageBuffer = data
 

  // Create a new payload
  var payload = [{
    topic: 'node-test',
    messages: messageBuffer,
    attributes: 1 /* Use GZip compression for the payload */
  }];

  //Send payload to Kafka and log result/error
  producer.send(payload, function(error, result) {
    console.info('Sent payload to Kafka: ', payload);
    if (error) {
      console.error(error);
    } else {
      var formattedResult = result[0]
      console.log('result: ', result)
    }
    });
    });
});

// For this demo we just log producer errors to the console.
producer.on('error', function(error) {
  console.error(error);
});