import ballerina/config;
import ballerina/log;
import ballerina/test;
import ballerina/time;
import ballerina/websub;
import ballerinax/java.jdbc;

jdbc:Client jdbcClient = new({
    url: config:getAsString("test.hub.db.url"),
    username: config:getAsString("test.hub.db.username"),
    password: config:getAsString("test.hub.db.password"),
    dbOptions: {
        useSSL: config:getAsString("test.hub.db.useSsl")
    }
});

MySqlHubPersistenceStore store = checkpanic new(jdbcClient, "random_test_keys".toBytes());

const TOPIC_ONE = "topicOne";
const TOPIC_TWO = "topicTwo";

final int timeOneOne = time:currentTime().time;
final int timeOneTwo = time:currentTime().time;
final int timeTwoOne = time:currentTime().time;

websub:SubscriptionDetails subOneOne = {
    topic: TOPIC_ONE,
    callback: "callback.one.one",
    secret: "QweTyuQw",
    leaseSeconds: 86400000,
    createdAt: timeOneOne
};

websub:SubscriptionDetails subOneTwo = {
    topic: TOPIC_ONE,
    callback: "callback.one.two",
    leaseSeconds: 32140000,
    createdAt: timeOneTwo
};

websub:SubscriptionDetails subTwoOne = {
    topic: TOPIC_TWO,
    callback: "callback.two.one",
    secret: "1asjfksnf112wsdf",
    leaseSeconds: 86400000,
    createdAt: timeTwoOne
};

@test:BeforeSuite
function setup() {
    dropTableData();
}

@test:Config {}
function testRetrieveTopicsWithNoTopics() {
    string[] topics = store->retrieveTopics();
    test:assertEquals(topics.length(), 0);
}

@test:Config {
    dependsOn: ["testRetrieveTopicsWithNoTopics"]
}
function testTopicRegistrationAndRetrieval() {
    store->addTopic(TOPIC_ONE);
    store->addTopic(TOPIC_TWO);

    string[] topics = store->retrieveTopics();
    test:assertEquals(topics.length(), 2);
    test:assertEquals(topics[0], TOPIC_ONE);
    test:assertEquals(topics[1], TOPIC_TWO);
}

@test:Config {
    dependsOn: ["testTopicRegistrationAndRetrieval"]
}
function testRetrieveSubscriptionsWithNoSubscriptions() {
    websub:SubscriptionDetails[] subs = store->retrieveAllSubscribers();
    test:assertEquals(subs.length(), 0);
}

@test:Config {
    dependsOn: ["testRetrieveSubscriptionsWithNoSubscriptions"]
}
function testSubscriptionRegistrationAndRetrieval() {
    store->addSubscription(subOneOne);
    store->addSubscription(subOneTwo);
    store->addSubscription(subTwoOne);

    websub:SubscriptionDetails[] subs = store->retrieveAllSubscribers();
    test:assertEquals(subs.length(), 3);

    boolean subOneOneExists = false;
    boolean subOneTwoExists = false;
    boolean subTwoOneExists = false;

    foreach var sub in subs {
        if (sub.topic == TOPIC_ONE) {
            if (sub == subOneOne) {
                subOneOneExists = true;
            } else if (sub == subOneTwo) {
                subOneTwoExists = true;
            } else {
                test:assertFail("unregistered subscriber found: " + sub.toString());
            }
        } else if (sub.topic == TOPIC_TWO) {
            subTwoOneExists = sub == subTwoOne;
        }
    }
    test:assertTrue(subOneOneExists && subOneTwoExists && subTwoOneExists);
}

@test:Config {
    dependsOn: ["testSubscriptionRegistrationAndRetrieval"]
}
function testSubscriptionRemoval() {
    store->removeSubscription(subOneOne);

    websub:SubscriptionDetails[] subs = store->retrieveAllSubscribers();
    test:assertEquals(subs.length(), 2);

    boolean subOneTwoExists = false;
    boolean subTwoOneExists = false;

    foreach var sub in subs {
        if (sub.topic == TOPIC_ONE) {
            if (sub == subOneTwo) {
                subOneTwoExists = true;
            } else {
                test:assertFail("unregistered subscriber found: " + sub.toString());
            }
        } else if (sub.topic == TOPIC_TWO) {
            subTwoOneExists = sub == subTwoOne;
        }
    }
    test:assertTrue(subOneTwoExists && subTwoOneExists);
}

@test:Config {
    dependsOn: ["testSubscriptionRemoval"]
}
function testTopicRemoval() {
    string[] topics = store->retrieveTopics();
    test:assertEquals(topics.length(), 2);
    test:assertEquals(topics[0], TOPIC_ONE);
    test:assertEquals(topics[1], TOPIC_TWO);

    store->removeTopic(TOPIC_TWO);
    topics = store->retrieveTopics();
    test:assertEquals(topics.length(), 1);
    test:assertEquals(topics[0], TOPIC_ONE);
}

@test:Config {
    dependsOn: ["testTopicRemoval"]
}
function testRemovalAll() {
    store->removeAll();

    websub:SubscriptionDetails[] subs = store->retrieveAllSubscribers();
    test:assertEquals(subs.length(), 0);

    string[] topics = store->retrieveTopics();
    test:assertEquals(topics.length(), 0);
}

@test:AfterSuite
function teardown() {
    dropTableData();
    checkpanic jdbcClient.stop();
}

function dropTableData() {
    jdbc:UpdateResult|error result = jdbcClient->update("DELETE FROM subscriptions");
    if (result is error) {
        log:printError("Error deleting from subscriptions table", result);
    }

    result = jdbcClient->update("DELETE FROM topics");
    if (result is error) {
        log:printError("Error deleting from topics table", result);
    }
}
