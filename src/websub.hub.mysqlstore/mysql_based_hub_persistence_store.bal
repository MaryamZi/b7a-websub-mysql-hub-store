// Copyright 2019
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import ballerina/log;
import ballerina/websub;
import ballerinax/java.jdbc;

const CREATE_TOPICS_TABLE = "CREATE TABLE IF NOT EXISTS topics (topic VARCHAR(255), PRIMARY KEY (topic))";
const INSERT_INTO_TOPICS = "INSERT INTO topics (topic) VALUES (?)";
const DELETE_FROM_TOPICS = "DELETE FROM topics WHERE topic=?";
const SELECT_ALL_FROM_TOPICS = "SELECT * FROM topics";

const CREATE_SUBSCRIPTIONS_TABLE = "CREATE TABLE IF NOT EXISTS subscriptions (topic VARCHAR(255), callback VARCHAR(255), secret VARCHAR(255), lease_seconds BIGINT, created_at BIGINT, PRIMARY KEY (topic, callback))";
const INSERT_INTO_SUBSCRIPTIONS = "INSERT INTO subscriptions (topic,callback,secret,lease_seconds,created_at) VALUES (?,?,?,?,?)";
const DELETE_FROM_SUBSCRIPTIONS = "DELETE FROM subscriptions WHERE topic=? AND callback=?";
const SELECT_FROM_SUBSCRIPTIONS = "SELECT topic, callback, secret, lease_seconds, created_at FROM subscriptions";

public type MySqlHubPersistenceStore object {

    *websub:HubPersistenceStore;

    private jdbc:Client jdbcClient;

    public function __init(jdbc:Client jdbcClient) returns error? {
        self.jdbcClient = jdbcClient;
        _ = check self.jdbcClient->update(CREATE_TOPICS_TABLE);
        _ = check self.jdbcClient->update(CREATE_SUBSCRIPTIONS_TABLE);
    }

    # Function to add or update subscription details.
    #
    # + subscriptionDetails - The details of the subscription to add or update
    public function addSubscription(websub:SubscriptionDetails subscriptionDetails) {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.topic};
        jdbc:Parameter para2 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.callback};
        jdbc:Parameter para3 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.secret};
        jdbc:Parameter para4 = {sqlType: jdbc:TYPE_BIGINT, value: subscriptionDetails.leaseSeconds};
        jdbc:Parameter para5 = {sqlType: jdbc:TYPE_BIGINT, value: subscriptionDetails.createdAt};

        var result = self.jdbcClient->update(DELETE_FROM_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully removed " + result.toString() + " entries for existing subscription");
        } else {
            log:printError("Error occurred deleting subscription data: " + getErrorMessageToLog(result));
        }

        result = self.jdbcClient->update(INSERT_INTO_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2,
                                         <@untainted>para3, <@untainted>para4, <@untainted>para5);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for subscription");
        } else {
            log:printError("Error occurred updating subscription data: " + getErrorMessageToLog(result));
        }
    }

    # Function to remove subscription details.
    #
    # + subscriptionDetails - The details of the subscription to remove
    public function removeSubscription(websub:SubscriptionDetails subscriptionDetails) {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.topic};
        jdbc:Parameter para2 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.callback};

        var result = self.jdbcClient->update(DELETE_FROM_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for unsubscription");
        } else {
            log:printError("Error occurred updating unsubscription data: " + getErrorMessageToLog(result));
        }
    }

    # Function to add a topic.
    #
    # + topic - The topic to add
    public function addTopic(string topic) {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: topic};

        var result = self.jdbcClient->update(INSERT_INTO_TOPICS, para1);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for topic registration");
        } else {
            log:printError("Error occurred updating topic registration data: " + getErrorMessageToLog(result));
        }
    }

    # Function to remove a topic.
    #
    # + topic - The topic to remove
    public function removeTopic(string topic) {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: topic};

        var result = self.jdbcClient->update(DELETE_FROM_TOPICS, para1);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for topic unregistration");
        } else {
            log:printError("Error occurred updating topic unregistration data: " + getErrorMessageToLog(result));
        }
    }

    # Function to retrieve all registered topics.
    #
    # + return - An array of topics
    public function retrieveTopics() returns string[] {
        string[] topics = [];
        var result = self.jdbcClient->select(SELECT_ALL_FROM_TOPICS, TopicRegistration);
        if (result is table<record {}>) {
            while (result.hasNext()) {
                TopicRegistration|error registrationDetails = trap <TopicRegistration> result.getNext();
                if (registrationDetails is TopicRegistration) {
                    topics[topics.length()] = registrationDetails.topic;
                } else {
                    log:printError("Error retreiving topic registration details from the database: " + 
                                    getErrorMessageToLog(registrationDetails));
                }
            }
        } else {
            log:printError("Error retreiving data from the database: " + getErrorMessageToLog(result));
        }
        return <@untainted> topics;
    }

    # Function to retrieve subscription details of all subscribers.
    #
    # + return - An array of subscriber details
    public function retrieveAllSubscribers() returns websub:SubscriptionDetails[] {
        websub:SubscriptionDetails[] subscriptions = [];
        var result = self.jdbcClient->select(SELECT_FROM_SUBSCRIPTIONS, websub:SubscriptionDetails);
        if (result is table<record {}>) {
            while (result.hasNext()) {
                var subscriptionDetails = trap <websub:SubscriptionDetails> result.getNext();
                if (subscriptionDetails is websub:SubscriptionDetails) {
                    subscriptions[subscriptions.length()] = subscriptionDetails;
                } else {
                    log:printError("Error retreiving subscription details from the database: " + 
                                    getErrorMessageToLog(subscriptionDetails));
                }
            }
        } else {
            log:printError("Error retreiving data from the database: " + getErrorMessageToLog(result));
        }
        return <@untainted> subscriptions;
    }
};

function getErrorMessageToLog(error e) returns string {
    return e.detail()?.message ?: e.reason();
}

type TopicRegistration record {|
    string topic;
|};
