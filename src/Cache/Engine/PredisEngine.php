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


namespace kbATeam\CakePhpPredis\Cache\Engine;

use Cake\Cache\Engine\RedisEngine;
use Predis\Client;

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
     * @param array<string, mixed> $settings array of setting for the engine
     * @return bool True if the engine has been successfully initialized, false if not
     */
    public function init($settings = array()): bool
    {
        if (!class_exists('\Predis\Client')) {
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

            foreach ($this->_config['sentinel'] as $host) {
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

            $parameters = [];
            foreach ($this->_config['server'] as $host) {
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
     * Delete a key from the cache
     *
     * @param string $key Identifier for the data
     * @return bool True if the value was successfully deleted, false if it didn't exist or couldn't be removed
     */
    public function delete($key): bool
    {
        $key = $this->_key($key);

        return (int)$this->_Redis->del($key) > 0;
    }

    /**
     * Delete all keys from the cache
     *
     * @param bool $check If true will check expiration, otherwise delete all.
     * @return bool True if the cache was successfully cleared, false otherwise
     */
    public function clear($check = false): bool
    {
        if ($check) {
            return true;
        }
        $keys = $this->_Redis->keys($this->_config['prefix'] . '*');

        $result = [];
        foreach ($keys as $key) {
            $result[] = (int)$this->_Redis->del($key) > 0;
        }

        return !in_array(false, $result);
    }
}
