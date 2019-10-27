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

import ballerina/crypto;
import ballerina/lang.array;
import ballerina/lang.'string;
import ballerina/log;
import ballerina/websub;
import ballerinax/java.jdbc;

const ERROR_REASON = "{maryamzi/websub.hub.mysqlstore}Error";

const CREATE_TOPICS_TABLE = "CREATE TABLE IF NOT EXISTS topics (topic VARCHAR(255), PRIMARY KEY (topic))";
const INSERT_INTO_TOPICS = "INSERT INTO topics (topic) VALUES (?)";
const DELETE_FROM_TOPICS = "DELETE FROM topics WHERE topic=?";
const SELECT_ALL_FROM_TOPICS = "SELECT * FROM topics";
const DELETE_ALL_TOPICS = "DELETE FROM topics";

const CREATE_SUBSCRIPTIONS_TABLE = "CREATE TABLE IF NOT EXISTS subscriptions (topic VARCHAR(255), callback VARCHAR(255), secret VARCHAR(255), lease_seconds BIGINT, created_at BIGINT, PRIMARY KEY (topic, callback))";
const INSERT_INTO_SUBSCRIPTIONS = "INSERT INTO subscriptions (topic,callback,secret,lease_seconds,created_at) VALUES (?,?,?,?,?)";
const DELETE_FROM_SUBSCRIPTIONS = "DELETE FROM subscriptions WHERE topic=? AND callback=?";
const SELECT_FROM_SUBSCRIPTIONS = "SELECT topic, callback, secret, lease_seconds, created_at FROM subscriptions";
const DELETE_ALL_SUBSCRIPTIONS = "DELETE FROM subscriptions";

public type MySqlHubPersistenceStore client object {

    *websub:HubPersistenceStore;

    private jdbc:Client jdbcClient;
    private byte[]? key;

    public function __init(jdbc:Client jdbcClient, byte[]? key = ()) returns error? {
        self.jdbcClient = jdbcClient;
        _ = check self.jdbcClient->update(CREATE_TOPICS_TABLE);
        _ = check self.jdbcClient->update(CREATE_SUBSCRIPTIONS_TABLE);

        if (key is byte[]) {
            int keyLength = key.length();
            if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
                return error(ERROR_REASON,
                             message = "invalid key length '" + keyLength .toString() + "', expected a key of length " +
                             "16, 24 or 32");
            }
        }
        self.key = key;
    }

    # Remote method to add or update subscription details.
    #
    # + subscriptionDetails - The details of the subscription to add or update
    # + return - `error` if addition failed, `()` otherwise
    public remote function addSubscription(websub:SubscriptionDetails subscriptionDetails) returns error? {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.topic};
        jdbc:Parameter para2 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.callback};

        string secret = subscriptionDetails.secret;
        byte[]? key = self.key;
        if (secret.trim() != "" && key is byte[]) {
            byte[] secretArr = subscriptionDetails.secret.toBytes();
            byte[]|error encryptedSecret = crypto:encryptAesEcb(secretArr, key);
            if (encryptedSecret is byte[]) {
                secret = encryptedSecret.toBase64();
            } else {
                return error(ERROR_REASON, message = "Error encrypting secret", cause = encryptedSecret);
            }
        }

        jdbc:Parameter para3 = {sqlType: jdbc:TYPE_VARCHAR, value: secret};
        jdbc:Parameter para4 = {sqlType: jdbc:TYPE_BIGINT, value: subscriptionDetails.leaseSeconds};
        jdbc:Parameter para5 = {sqlType: jdbc:TYPE_BIGINT, value: subscriptionDetails.createdAt};

        var result = self.jdbcClient->update(DELETE_FROM_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully removed " + result.toString() + " entries for existing subscription");
        } else {
            return error(ERROR_REASON, message = "Error occurred deleting subscription data", cause = result);
        }

        result = self.jdbcClient->update(INSERT_INTO_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2,
                                         <@untainted>para3, <@untainted>para4, <@untainted>para5);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for subscription");
        } else {
            return error(ERROR_REASON, message = "Error occurred updating subscription data", cause = result);
        }
    }

    # Remote method to remove subscription details.
    #
    # + subscriptionDetails - The details of the subscription to remove
    # + return - `error` if removal failed, `()` otherwise
    public remote function removeSubscription(websub:SubscriptionDetails subscriptionDetails) returns error? {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.topic};
        jdbc:Parameter para2 = {sqlType: jdbc:TYPE_VARCHAR, value: subscriptionDetails.callback};

        var result = self.jdbcClient->update(DELETE_FROM_SUBSCRIPTIONS, <@untainted>para1, <@untainted>para2);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for unsubscription");
        } else {
            return error(ERROR_REASON, message = "Error occurred updating unsubscription data", cause = result);
        }
    }

    # Remote method to add a topic.
    #
    # + topic - The topic to add
    # + return - `error` if addition failed, `()` otherwise
    public remote function addTopic(string topic) returns error? {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: topic};

        var result = self.jdbcClient->update(INSERT_INTO_TOPICS, para1);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for topic registration");
        } else {
            return error(ERROR_REASON, message = "Error occurred updating topic registration data", cause = result);
        }
    }

    # Remote method to remove a topic.
    #
    # + topic - The topic to remove
    # + return - `error` if removal failed, `()` otherwise
    public remote function removeTopic(string topic) returns error? {
        jdbc:Parameter para1 = {sqlType: jdbc:TYPE_VARCHAR, value: topic};

        var result = self.jdbcClient->update(DELETE_FROM_TOPICS, para1);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully updated " + result.toString() + " entries for topic unregistration");
        } else {
            return error(ERROR_REASON, message = "Error occurred updating topic unregistration data", cause = result);
        }
    }

    # Remote method to retrieve all registered topics.
    #
    # + return - `error` if retrieval failed, `string[]` otherwise
    public remote function retrieveTopics() returns @tainted string[]|error {
        string[] topics = [];
        var result = self.jdbcClient->select(SELECT_ALL_FROM_TOPICS, TopicRegistration);
        if (result is table<record {}>) {
            while (result.hasNext()) {
                TopicRegistration|error registrationDetails = trap <TopicRegistration> result.getNext();
                if (registrationDetails is TopicRegistration) {
                    topics[topics.length()] = registrationDetails.topic;
                } else {
                    return error(ERROR_REASON,
                                 message = "Error retreiving topic registration details from the database",
                                 cause = registrationDetails);
                }
            }
        } else {
            return error(ERROR_REASON, message = "Error retreiving topic data from the database", cause = result);
        }
        return <@untainted> topics;
    }

    # Remote method to retrieve subscription details of all subscribers.
    #
    # + return - `error` if addition failed, `websub:SubscriptionDetails[]` otherwise
    public remote function retrieveAllSubscribers() returns @tainted websub:SubscriptionDetails[]|error {
        websub:SubscriptionDetails[] subscriptions = [];
        var result = self.jdbcClient->select(SELECT_FROM_SUBSCRIPTIONS, websub:SubscriptionDetails);
        if (result is table<record {}>) {
            while (result.hasNext()) {
                var subscriptionDetails = trap <websub:SubscriptionDetails> result.getNext();
                if (subscriptionDetails is websub:SubscriptionDetails) {
                    byte[]? key = self.key;
                    if (subscriptionDetails.secret.trim() != "" && key is byte[]) {
                        byte[]|error encryptedSecretAsByteArr = array:fromBase64(subscriptionDetails.secret);
                        if (encryptedSecretAsByteArr is byte[]) {
                            byte[]|error decryptedSecretArr = crypto:decryptAesEcb(encryptedSecretAsByteArr, key);

                            if (decryptedSecretArr is byte[]) {
                                string|error decryptedSecretString = 'string:fromBytes(decryptedSecretArr);
                                if (decryptedSecretString is string) {
                                    subscriptionDetails.secret = decryptedSecretString;
                                } else {
                                    return error(ERROR_REASON,
                                                 message = "Error converting decrypted secret byte[] to string",
                                                 cause = decryptedSecretString);
                                }
                            } else {
                                return error(ERROR_REASON, message = "Error decrypting secret",
                                             cause = decryptedSecretArr);
                            }
                        } else {
                            return error(ERROR_REASON, message = "Error converting encrypted secret string to byte[]",
                                         cause = encryptedSecretAsByteArr);
                        }
                    }
                    subscriptions[subscriptions.length()] = subscriptionDetails;
                } else {
                    return error(ERROR_REASON, message = "Error retreiving subscription details from the database",
                                 cause = subscriptionDetails);
                }
            }
        } else {
            return error(ERROR_REASON, message = "Error retreiving subscription data from the database", cause = result);
        }
        return <@untainted> subscriptions;
    }
    
    # Remote method to delete all the registered topics.
    #
    # + return - `error` if removal failed, `()` otherwise
    public remote function removeTopics() returns error? {
        var result = self.jdbcClient->update(DELETE_ALL_TOPICS);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully removed all topic entries");
        } else {
            return error(ERROR_REASON, message = "Error occurred deleting all topics", cause = result);
        }
    }

    # Remote method to delete all the registered subscriptions.
    #
    # + return - `error` if removal failed, `()` otherwise
    public remote function removeSubscriptions() returns error? {
        var result = self.jdbcClient->update(DELETE_ALL_SUBSCRIPTIONS);
        if (result is jdbc:UpdateResult) {
            log:printDebug("Successfully removed all subscription entries");
        } else {
            return error(ERROR_REASON, message = "Error occurred deleting all subscriptions", cause = result);
        }
    }

    # Remote method to delete all the registered topics and subscriptions.
    #
    # + return - `error` if removal failed, `()` otherwise
    public remote function removeAll() returns error? {
        check self->removeSubscriptions();
        check self->removeTopics();
    }
};

type TopicRegistration record {|
    string topic;
|};
