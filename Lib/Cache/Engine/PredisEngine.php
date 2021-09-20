<?php

/**
 * Redis storage engine for cache (sentinel support)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Martin M.
 * @link          https://github.com/the-kbA-team/cakephp-predis
 * @package       RedisCluster.Lib.Cache.Engine
 * @license       https://github.com/the-kbA-team/cakephp-predis/blob/main/LICENSE
 */

App::uses('RedisEngine', 'Cache/Engine');

/**
 * Redis storage engine for cache (cluster support)
 *
 * @package       Cake.Cache.Engine
 */
class PredisEngine extends RedisEngine
{
    /**
     * Initialize the Cache Engine
     *
     * Called automatically by the cache frontend
     * To reinitialize the settings call Cache::engine('EngineName', [optional] settings = array());
     *
     * @param array $settings array of setting for the engine
     * @return bool True if the engine has been successfully initialized, false if not
     */
    public function init($settings = array())
    {
        if (!class_exists('Predis\Client')) {
            return false;
        }

        return parent::init(
            array_merge([
                'engine' => 'Predis',
                'scheme' => 'tcp',
                'prefix' => '',
                'server' => null,
                'sentinel' => null,
                'port' => 6379,
                'database' => 0,
                'password' => null,
                'service' => 'mymaster'
            ], $settings)
        );
    }

    /**
     * Connects to a Redis server
     *
     * @return bool True if Redis server was connected
     */
    protected function _connect()
    {
        if (empty($this->settings['server']) && empty($this->settings['sentinel'])) {
            throw new Exception('No redis server configured!');
        }

        if (!empty($this->settings['server']) && !empty($this->settings['sentinel'])) {
            throw new Exception('Both sentinel and server is set!');
        }

        $parameters = [];
        $options = [];

        /**
         * Sentinel configuration
         */
        if (!empty($this->settings['sentinel'])) {
            if (is_string($this->settings['sentinel'])) {
                $this->settings['sentinel'] = [$this->settings['sentinel']];
            }

            $parameters = [];
            foreach ($this->settings['sentinel'] as $host) {
                $node = [
                    'scheme' => $this->settings['scheme'],
                    'host' =>  $host,
                    'port' => $this->settings['port'],
                    'password' => $this->settings['password'],
                ];
                $parameters[] = $node;
            }

            $options = [
                'replication' => 'sentinel',
                'service' => $this->settings['service'],
                'parameters' => [
                    'password' => $this->settings['password'],
                    'database' => $this->settings['database'],
                ],
            ];
        }

        /**
         * Master/slave configuration
         */
        if (!empty($this->settings['server'])) {
            $options = [
                'replication' => 'predis',
                'parameters' => [
                    'password' => $this->settings['password'],
                    'database' => $this->settings['database'],
                ],
            ];
            if (is_string($this->settings['server'])) {
                $this->settings['server'] = [$this->settings['server']];
                $options = [];
            }

            $parameters = [];
            foreach ($this->settings['server'] as $host) {
                $node = [
                    'scheme' => $this->settings['scheme'],
                    'host' =>  $host,
                    'port' => $this->settings['port'],
                    'password' => $this->settings['password'],
                ];
                //$node = sprintf("%s://%s", $this->settings['scheme'], $host);
                $parameters[] = $node;
            }
        }

        try {
            $this->_Redis = new Predis\Client($parameters, $options);
        } catch (Predis\CommunicationException $e) {
            return false;
        }

        return true;
    }

    /**
     * Write data for key into cache.
     *
     * @param string $key Identifier for the data
     * @param mixed $value Data to be cached
     * @param int $duration How long to cache the data, in seconds
     * @return bool True if the data was successfully cached, false on failure
     */
    public function write($key, $value, $duration)
    {
        if (!is_int($value)) {
            $value = serialize($value);
        }
        if ($duration === 0) {
            return $this->_Redis->set($key, $value);
        }

        return $this->_Redis->setex($key, $duration, $value);
    }

    public function delete($key)
    {
        return $this->_Redis->del($key);
    }

    /**
     * Delete all keys from the cache
     *
     * @param bool $check Whether or not expiration keys should be checked. If
     *   true, no keys will be removed as cache will rely on redis TTL's.
     * @return bool True if the cache was successfully cleared, false otherwise
     */
    public function clear($check)
    {
        if ($check) {
            return true;
        }
        $keys = $this->_Redis->keys($this->settings['prefix'] . '*');
        if (!empty($keys)) {
            $this->_Redis->del($keys);
        }

        return true;
    }
}
