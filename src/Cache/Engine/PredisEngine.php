<?php
declare(strict_types=1);


/**
 * Predis storage engine for cache (sentinel support)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Martin M.
 * @link          https://github.com/the-kbA-team/cakephp-predis
 * @package       RedisCluster.Lib.Cache.Engine
 * @license       https://github.com/the-kbA-team/cakephp-predis/blob/main/LICENSE
 */

namespace kbATeam\CakePhpPredis\Cache\Engine;

use Cake\Cache\CacheEngine;
use Predis\Client;
use Redis;


/**
 * Redis storage engine for cache (cluster support)
 *
 * @package       Cake.Cache.Engine
 */
class PredisEngine extends CacheEngine
{
    /**
     * @var Client
     */
    protected  Client $_Redis;

    /**
     * The default config used unless overridden by runtime configuration
     *
     * - `database` database number to use for connection.
     * - `duration` Specify how long items in this cache configuration last.
     * - `groups` List of groups or 'tags' associated to every key stored in this config.
     *    handy for deleting a complete group from cache.
     * - `password` Redis server password.
     * - `persistent` Connect to the Redis server with a persistent connection
     * - `port` port number to the Redis server.
     * - `prefix` Prefix appended to all entries. Good for when you need to share a keyspace
     *    with either another cache config or another application.
     * - `scanCount` Number of keys to ask for each scan (default: 10)
     * - `server` URL or IP to the Redis server host.
     * - `timeout` timeout in seconds (float).
     * - `unix_socket` Path to the unix socket file (default: false)
     *
     * @var array<string, mixed>
     */
    protected array $_defaultConfig = [
        'engine' => 'Predis',
        'scheme' => 'tcp',
        'service' => 'mymaster',
        'sentinel' => null,
        'database' => 0,
        'duration' => 3600,
        'groups' => [],
        'password' => false,
        'persistent' => true,
        'server' => null,
        'port' => 6379,
        'prefix' => 'cake_',
        'timeout' => 0,
        'unix_socket' => false,
        'scanCount' => 10,
    ];

    /**
     * Initialize the Cache Engine
     *
     * Called automatically by the cache frontend
     *
     * @param array<string, mixed> $config array of setting for the engine
     * @return bool True if the engine has been successfully initialized, false if not
     */
    public function init($config = array()): bool
    {
        if (!class_exists('\Predis\Client')) {
            return false;
        }

        parent::init($config);
        return $this->_connect();
    }

    /**
     * Connects to a Redis/Sentinel
     *
     * @return bool True if Redis server was connected
     * @throws \Exception
     *
     */
    protected function _connect(): bool
    {
        if (empty($this->_config['server']) && empty($this->_config['sentinel'])) {
            throw new \Exception('No redis server configured!');
        }

        if (!empty($this->_config['server']) && !empty($this->_config['sentinel'])) {
            throw new \Exception('Both sentinel and server is set!');
        }

        $parameters = [];
        $options = [];

        /**
         * Sentinel configuration
         */
        if (!empty($this->_config['sentinel'])) {
            if (is_string($this->_config['sentinel'])) {
                $this->_config['sentinel'] = [$this->_config['sentinel']];
            }
            /** @var array<int, string> $sentinel */
            $sentinel = $this->_config['sentinel'];

            foreach ($sentinel as $host) {
                $node = [
                    'scheme' => $this->_config['scheme'],
                    'host' => $host,
                    'port' => $this->_config['port'],
                    'password' => $this->_config['password'],
                ];
                $parameters[] = $node;
            }

            $options = [
                'replication' => 'sentinel',
                'service' => $this->_config['service'],
                'parameters' => [
                    'password' => $this->_config['password'],
                    'database' => $this->_config['database'],
                ],
            ];
        }

        /**
         * Master/slave configuration
         */
        if (!empty($this->_config['server'])) {
            $options = [
                'replication' => 'predis',
                'parameters' => [
                    'password' => $this->_config['password'],
                    'database' => $this->_config['database'],
                ],
            ];
            if (is_string($this->_config['server'])) {
                $this->_config['server'] = [$this->_config['server']];
                $options = [];
            }

            /** @var array<int, string> $serverList */
            $serverList = $this->_config['server'];

            $parameters = [];
            foreach ($serverList as $host) {
                $node = [
                    'scheme' => $this->_config['scheme'],
                    'host' => $host,
                    'port' => $this->_config['port'],
                    'password' => $this->_config['password'],
                ];
                //$node = sprintf("%s://%s", $this->_config['scheme'], $host);
                $parameters[] = $node;
            }
        }

        /**
         * should exceptions be thrown
         */
        if (isset($this->_config['exceptions'])) {
            $options['exceptions'] = (bool)$this->_config['exceptions'];
        }

        try {
            $this->_Redis = new \Predis\Client($parameters, $options);
        } catch (\Predis\CommunicationException $e) {
            return false;
        }

        return true;
    }


    /**
     * Write data for key into cache.
     *
     * @param string $key Identifier for the data
     * @param mixed $value Data to be cached
     * @param \DateInterval|int|null $ttl Optional. The TTL value of this item. If no value is sent and
     *   the driver supports TTL then the library may set a default value
     *   for it or let the driver take care of that.
     * @return bool True if the data was successfully cached, false on failure
     */
    public function set(string $key, mixed $value, \DateInterval|int|null $ttl = null): bool
    {
        $key = $this->_key($key);
        $value = $this->serialize($value);

        $duration = $this->duration($ttl);
        if ($duration === 0) {
            return (bool)$this->_Redis->set($key, $value);
        }

        return (bool)$this->_Redis->setEx($key, $duration, $value);
    }

    /**
     * Read a key from the cache
     *
     * @param string $key Identifier for the data
     * @param mixed $default Default value to return if the key does not exist.
     * @return mixed The cached data, or the default if the data doesn't exist, has
     *   expired, or if there was an error fetching it
     */
    public function get(string $key, mixed $default = null): mixed
    {
        $value = $this->_Redis->get($this->_key($key));
        if ($value === null) {
            return $default;
        }

        return $this->unserialize($value);
    }

    /**
     * Increments the value of an integer cached key & update the expiry time
     *
     * @param string $key Identifier for the data
     * @param int $offset How much to increment
     * @return int|false New incremented value, false otherwise
     */
    public function increment(string $key, int $offset = 1): int|false
    {
        $duration = $this->_config['duration'];
        $key = $this->_key($key);

        $value = $this->_Redis->incrBy($key, $offset);
        if ($duration > 0) {
            $this->_Redis->expire($key, $duration);
        }

        return $value;
    }

    /**
     * Decrements the value of an integer cached key & update the expiry time
     *
     * @param string $key Identifier for the data
     * @param int $offset How much to subtract
     * @return int|false New decremented value, false otherwise
     */
    public function decrement(string $key, int $offset = 1): int|false
    {
        $duration = $this->_config['duration'];
        $key = $this->_key($key);

        $value = $this->_Redis->decrBy($key, $offset);
        if ($duration > 0) {
            $this->_Redis->expire($key, $duration);
        }

        return $value;
    }

    /**
     * Delete a key from the cache
     *
     * @param string $key Identifier for the data
     * @return bool True if the value was successfully deleted, false if it didn't exist or couldn't be removed
     */
    public function delete(string $key): bool
    {
        $key = $this->_key($key);

        return (int)$this->_Redis->del($key) > 0;
    }

    /**
     * Delete a key from the cache asynchronously
     *
     * Just unlink a key from the cache. The actual removal will happen later asynchronously.
     *
     * @param string $key Identifier for the data
     * @return bool True if the value was successfully deleted, false if it didn't exist or couldn't be removed
     */
    public function deleteAsync(string $key): bool
    {
        $key = $this->_key($key);

        return (int)$this->_Redis->unlink($key) > 0;
    }

    /**
     * Delete all keys from the cache
     *
     * @param bool $check
     * @return bool
     */
    public function clear($check = false): bool
    {
        if ($check) {
            return true;
        }

        $keys = $this->_Redis->keys($this->_config['prefix'] . '*');

        $isAllDeleted = true;
        foreach ($keys as $key) {
            $isDeleted = ((int)$this->_Redis->del($key) > 0);
            $isAllDeleted = $isAllDeleted && $isDeleted;
        }

        return $isAllDeleted;
    }

    /**
     * Delete all keys from the cache by a blocking operation
     *
     * Faster than clear() using unlink method.
     *
     * @return bool True if the cache was successfully cleared, false otherwise
     */
    public function clearBlocking(): bool
    {
        $this->_Redis->setOption(Redis::OPT_SCAN, (string)Redis::SCAN_RETRY);

        $isAllDeleted = true;
        $iterator = null;
        $pattern = [$this->_config['prefix'] . '*'];

        while (true) {
            $keys = $this->_Redis->scan($iterator, $pattern);

            if ($keys === false) {
                break;
            }

            foreach ($keys as $key) {
                $isDeleted = ((int)$this->_Redis->unlink($key) > 0);
                $isAllDeleted = $isAllDeleted && $isDeleted;
            }
        }

        return $isAllDeleted;
    }

    /**
     * Write data for key into cache if it doesn't exist already.
     * If it already exists, it fails and returns false.
     *
     * @param string $key Identifier for the data.
     * @param mixed $value Data to be cached.
     * @return bool True if the data was successfully cached, false on failure.
     * @link https://github.com/phpredis/phpredis#set
     */
    public function add(string $key, mixed $value): bool
    {
        $duration = $this->_config['duration'];
        $key = $this->_key($key);
        $value = $this->serialize($value);

        if ($this->_Redis->set($key, $value, ['nx', 'ex' => $duration])) {
            return true;
        }

        return false;
    }

    /**
     * Returns the `group value` for each of the configured groups
     * If the group initial value was not found, then it initializes
     * the group accordingly.
     *
     * @return array<string>
     */
    public function groups(): array
    {
        $result = [];
        /** @var array<int, string> $groups */
        $groups = $this->_config['groups'];
        foreach ($groups as $group) {
            $value = $this->_Redis->get($this->_config['prefix'] . $group);
            if (!$value) {
                $value = $this->serialize(1);
                $this->_Redis->set($this->_config['prefix'] . $group, $value);
            }
            $result[] = $group . $value;
        }

        return $result;
    }

    /**
     * Increments the group value to simulate deletion of all keys under a group
     * old values will remain in storage until they expire.
     *
     * @param string $group name of the group to be cleared
     * @return bool success
     */
    public function clearGroup(string $group): bool
    {
        return (bool)$this->_Redis->incr($this->_config['prefix'] . $group);
    }

    /**
     * Serialize value for saving to Redis.
     *
     * This is needed instead of using Redis' in built serialization feature
     * as it creates problems incrementing/decrementing intially set integer value.
     *
     * @param mixed $value Value to serialize.
     * @return string
     * @link https://github.com/phpredis/phpredis/issues/81
     */
    protected function serialize(mixed $value): string
    {
        if (is_int($value)) {
            return (string)$value;
        }

        return serialize($value);
    }

    /**
     * Unserialize string value fetched from Redis.
     *
     * @param string $value Value to unserialize.
     * @return mixed
     */
    protected function unserialize(string $value): mixed
    {
        if (preg_match('/^[-]?\d+$/', $value)) {
            return (int)$value;
        }

        return unserialize($value);
    }

    /**
     * Disconnects from the redis server
     * @return void
     */
    public function __destruct()
    {
        if (empty($this->_config['persistent'])) {
            $this->_Redis->close();
        }
    }
}
