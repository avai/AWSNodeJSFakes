var topics = {};

module.exports.createTopic = function(params, cb) {
    cb(null, {
        TopicArn: params.Name
    })
};
module.exports.publish = function(params, cb) {
    var topicArn = params.TopicArn;
    var publishedEvents = topics[topicArn];
    if (!publishedEvents) {
        publishedEvents = [];
        topics[topicArn] = publishedEvents;
    }

    publishedEvents.push(params.Message);
    cb(null);
};
module.exports.getPublishedEventsForTopic = function (topicName) {
    return topics[topicName];
};
module.exports.reset = function () {
    topics = {};
}
module.exports.printAllTopics = function () {
    var prefix = "";
        for ( var i in topics ) {
        var events = topics[i];
        prefix += "\t";
        console.log(i + " - " + events.length + " events");
        for ( var j in events ) {
            console.log(prefix + j + ":" );
            console.log(JSON.stringify(JSON.parse(events[j]), null, '\t'));
        }
    }
}