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

module.exports = adapter;

/**
 * Gets or creates a Pub/Sub topic
 * @param {Object} pubsub A gcloud Pub/Sub object
 * @param {String} name Topic name
 * @param {Function} callback Callback function (err, topic)
 */
function getTopic(pubsub, name, callback) {
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

/**
 * Returns a pubsub Adapter class.
 *
 * @param {Object} pubsub A gcloud Pub/Sub object
 * @param {Object} opts Adapter options
 * @param {String} [opts.key=socket.io] The topic name of the Pub/Sub events
 * @return {PubsubAdapter} adapter
 * @api public
 */

function adapter(pubsub, opts) {
  opts = opts || {};

  const prefix = opts.key || 'socket.io';
  const uid = `socket.io-${shortid.generate()}`;
  const createSubscriptionOpts = opts.createSubscriptionOpts || {};

  /**
   * PubsubAdapter constructor.
   *
   * @param {String} nsp name
   * @api public
   */

  function PubsubAdapter(nsp) {
    this.original = new Adapter(nsp);

    this.uid = uid;
    this.pubsub = pubsub;
    this.prefix = prefix;
    this.channel = prefix + '#' + nsp.name + '#';
    this.topics = {};
    this.createSubscriptionOpts = createSubscriptionOpts;
    this.subscriptions = {};

    this.subscribe(this.prefix, () => {});
  }

  /**
   * Inherits from `Adapter`.
   */

  PubsubAdapter.prototype.__proto__ = Adapter.prototype;

  /**
   * Wait for initialization to complete
   * @param {Function} callback Callback function
   */
  PubsubAdapter.prototype.waitForInitialization = function (callback) {
    if (this.initializing) {
      setTimeout(this.waitForInitialization.bind(this, callback));
    } else {
      callback();
    }
  };

  /**
   * Subscribe to a Pub/Sub topic
   * @param {String} topic The Pub/Sub topic
   * @param {Function} [callback] Callback function (err)
   * @api private
   */

  PubsubAdapter.prototype.subscribe = function (topic, callback) {
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
  };

  /**
   * Publish a new message to the Pub/Sub
   * @param {String} topic Topic name
   * @param {string} channel Channel name
   * @param {Object} message Message object
   */

  PubsubAdapter.prototype.publish = function (topic, channel, message) {
    // new pub sub publish method
    let dataBuffer = Buffer.from(JSON.stringify(Object.assign({}, message, { channel })));
    this.topics[topic].publish(dataBuffer, (err) => {
      if (err) {
        this.emit('error', err);
      }
    });
  };

  /**
   * Called with a subscription message
   *
   * @api private
   */

  PubsubAdapter.prototype.onmessage = function (subscription, msg) {
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
  };

  /**
   * Broadcasts a packet.
   *
   * @param {Object} packet to emit
   * @param {Object} opts
   * @param {Boolean} remote whether the packet came from another node
   * @api public
   */

  PubsubAdapter.prototype.broadcast = function (packet, opts, remote) {
    this.original.broadcast(packet, opts);

    this.waitForInitialization(() => {
      if (!remote) {
        const chn = prefix + '#' + packet.nsp + '#';
        const msg = {
          senderId: this.uid,
          packet,
          opts
        };

        if (opts.rooms && opts.rooms.length) {
          opts.rooms.forEach((room) => {
            const chnRoom = chn + room + '#';
            this.publish(this.prefix, chnRoom, msg);
          });
        } else {
          this.publish(this.prefix, chn, msg);
        }
      }
    });
  };

  /**
   * Adds a socket to a list of rooms.
   *
   * @param {String} id socket id
   * @param {String} rooms list of room names
   * @param {Function} callback
   * @api public
   */

  PubsubAdapter.prototype.addAll = function (id, rooms, callback) {
    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => this.original.addAll(id, rooms, callback),
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
  };

  /**
   * Deletes the pubsub subscription if no roo
   * @param callback
   */
  PubsubAdapter.prototype.lazyDeleteSubscription = function (callback) {
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
  };

  /**
   * Removes a socket from a room.
   *
   * @param {String} id socket id
   * @param {String} room name of the room
   * @param {Function} callback
   * @api public
   */

  PubsubAdapter.prototype.del = function (id, room, callback) {
    debug('removing %s from %s', id, room);

    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => this.original.del(id, room, callback),
        this.lazyDeleteSubscription.bind(this)
      ],
      callback
    );
  };

  /**
   * Removes a socket from all rooms it's joined.
   *
   * @param {String} id socket id
   * @param {Function} callback
   * @api public
   */

  PubsubAdapter.prototype.delAll = function (id, callback) {
    debug('removing %s from all rooms', id);
    async.waterfall(
      [
        this.waitForInitialization.bind(this),
        (callback) => this.original.delAll(id, callback),
        this.lazyDeleteSubscription.bind(this)
      ],
      callback
    );
  };

  return PubsubAdapter;
}
