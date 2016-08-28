var fakeAws = require('./index.js');

var fakeSns = fakeAws.FakeSns;
var fakeDocClient = fakeAws.FakeDocClient;

fakeSns.createTopic({ Name : "test-topic"}, function(err, data) {
    console.log('Created topic ' + data.TopicArn);
})

fakeDocClient.createTable("test-table", ['hash', 'sort']);
fakeDocClient.printDb();