'use strict';

/**
 * Module dependencies.
 */

const shortid = require('shortid');
const { Adapter } = require('socket.io-adapter');
const debug = require('debug')('socket.io-pubsub');
const async = require('async');

/**
 * Module exports.
 */

module.exports = exports = createAdapter;

function createAdapter(pubsub, opts = {}) {
  return function (nsp) {
    return new PubSubAdapter(nsp, pubsub, opts);
  };
}

function checkIfTopicExists(pubsub, name) {
  const exists = false;

  pubsub
    .getTopics()
    .then(function ([topics]) {
      topics.forEach((topic) => {
        if (topic.name === name) {
          exists = true;
        }
      });
    })
    .catch(function () {
      exists = false;
    });

  return exists;
}

/**
 * Gets or creates a Pub/Sub topic
 * @param {Object} pubsub A gcloud Pub/Sub object
 * @param {String} name Topic name
 * @param {Function} callback Callback function (err, topic)
 */
function getTopic(pubsub, name, callback) {
  if (checkIfTopicExists(pubsub, name)) return callback(null, pubsub.topic(name));

  pubsub.createTopic(name, (err, topic) => {
    // topic already exists.
    if (err && err.code === 6) {
      return callback(null, pubsub.topic(name));
    }
    return callback(err, topic);
  });
}

/**
 * Creates a Pub/Sub subscription
 * @param {String} name Subscription name
 * @param {Object} config Subscription config
 * @param {Object} topic Topic to subscribe to
 * @param {Function} callback Callback function (err, subscription)
 */
function getSubscription(name, config, topic, callback) {
  topic.createSubscription(name, config, callback);
}

/**
 * Checks whether a message channel matches the subscribed channel
 * @param {String} messageChannel The message channel
 * @param {String} subscribedChannel The subscribed channel
 * @returns {boolean} Whether they match or not
 */
function channelMatches(messageChannel, subscribedChannel) {
  return messageChannel.startsWith(subscribedChannel);
}

class PubSubAdapter extends Adapter {
  constructor(nsp, pubsub, opts = {}) {
    super(nsp);

    this.uid = `socket.io-${shortid.generate()}`;
    this.pubsub = pubsub;
    this.prefix = (opts && opts.key) || 'socket.io';
    this.channel = this.prefix + '#' + nsp.name + '#';
    this.topics = {};
    this.createSubscriptionOpts = (opts && opts.createSubscriptionOpts) || {};
    this.subscriptions = {};

    this.subscribe(this.prefix, () => {});
  }

  waitForInitialization(callback) {
    if (this.initializing) {
      setTimeout(this.waitForInitialization.bind(this, callback));
    } else {
      callback();
    }
  }

  subscribe(topic, callback) {
    const self = this;
    this.initializing = true;

    async.waterfall(
      [
        getTopic.bind(null, this.pubsub, topic),

        (topicObj, callback) => {
          self.topics[topic] = topicObj;
          callback(null, topicObj);
        },

        getSubscription.bind(null, this.uid, this.createSubscriptionOpts)
      ],
      (err, subscription) => {
        if (err) {
          this.emit('error', err);
        } else {
          this.subscriptions[topic] = subscription;
          this.deleting = false;
          this.initializing = false;
          subscription.on('message', this.onmessage.bind(this, subscription));
          subscription.on('error', (err) => {
            if (![404, 5].includes(err.code)) {
              throw err;
            }
          });
        }

        if (callback) {
          callback(err);
        }
      }
    );
  }

  publish(topic, channel, message) {
    // new pub sub publish method
    let dataBuffer = Buffer.from(JSON.stringify(Object.assign({}, message, { channel })));
    this.topics[topic].publish(dataBuffer, (err) => {
      if (err) {
        this.emit('error', err);
      }
    });
  }

  onmessage(subscription, msg) {
    if (subscription.closed) {
      return debug('subscription %s is closed', subscription.name);
    }

    const data = JSON.parse(Buffer.from(msg.data, 'base64').toString('utf8'));

    const channel = data.channel;

    if (!channelMatches(channel, this.channel)) {
      return debug('ignore different channel');
    }

    if (this.uid === data.senderId) return debug('ignore same uid');

    const packet = data.packet;

    if (packet && packet.nsp === undefined) {
      packet.nsp = '/';
    }

    if (!packet || packet.nsp !== this.nsp.name) {
      return debug('ignore different namespace');
    }

    this.broadcast(packet, data.opts, true);

    debug('acknowledging message %s of subscription %s', msg.id, subscription.name);
    msg.ack();
  }

  broadcast(packet, opts, remote) {
    super.broadcast(packet, opts);

    this.waitForInitialization(() => {
      if (!remote) {
        const chn = this.prefix + '#' + packet.nsp + '#';
        const msg = {
          senderId: this.uid,
          packet,
          opts
        };

        if (opts && opts.rooms && opts.rooms.length) {
          opts.rooms.forEach((room) => {
            const chnRoom = chn + room + '#';
            this.publish(this.prefix, chnRoom, msg);
          });
        } else {
          this.publish(this.prefix, chn, msg);
        }
      }
    });
  }

  addAll(id, rooms, callback) {
    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => super.addAll(id, rooms, callback),
        (callback) => {
          if (!this.subscriptions[this.prefix]) {
            this.subscribe(this.prefix, callback);
          } else {
            callback();
          }
        }
      ],
      callback
    );
  }

  lazyDeleteSubscription(callback) {
    if (!Object.keys(this.rooms || {}).length && !this.deleting) {
      debug('deleting subscriptions %s of topic %s', this.uid, this.prefix);
      this.deleting = true;
      this.subscriptions[this.prefix].delete((err) => {
        if (err && err.code !== 404) {
          this.emit('error', err);
        } else {
          err = null;
          delete this.subscriptions[this.prefix];
        }
        callback(err);
      });
    } else {
      callback();
    }
  }

  del(id, room, callback) {
    debug('removing %s from %s', id, room);

    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => super.del(this, id, room, callback),
        this.lazyDeleteSubscription.bind(this)
      ],
      callback
    );
  }

  delAll(id, callback) {
    debug('removing %s from all rooms', id);
    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => super.delAll(this, id, callback),
        this.lazyDeleteSubscription.bind(this)
      ],
      callback
    );
  }
}
