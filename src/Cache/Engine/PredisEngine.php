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
     * @throws Exception
     *
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
                    'host' => $host,
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
                    'host' => $host,
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
}
