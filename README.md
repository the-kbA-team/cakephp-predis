# CakePHP Predis

[![License: MIT][license-mit]](LICENSE)
[![Packagist Version][packagist-badge]][packagist]

Predis plugin for CakePHP 5.x

## Usage

```bash
composer require kba-team/cakephp-predis
```

In your configuration file add ...

```php
\Cake\Core\Configure::write('Cache._session_', [
        'className' => kbATeam\CakePhpPredis\Cache\Engine\PredisEngine::class,
        'sentinel' => ['<sentinel host 1>', ...., '<sentinel host n>'],
        'password' => "<password>",
        'port' => 26379,
        'exceptions' => true,
        'database' => 1,
        'prefix' => "",
        'duration' => '+2 days', 
])
```

Possible configuration parameters are:
* `className`  kbATeam\CakePhpPredis\Cache\Engine\PredisEngine::class
* `scheme`  Currently, TCP only
* `prefix`  See CakePhp 3.x Caching
* `server`   Redis server (must be master),
* `sentinel`  List of sentinel nodes (hostnames or IP addresses)
* `port`    Either redis Port (6379) is `server` is used otherwise the sentinel port (26379)
* `exceptions` Should exceptions be thrown or not (true)
* `database` See CakePhp 3.x Caching
* `password` Redis password
* `service`  Sentinel only: Sentinel service name (mymaster)

### Further reading

* About [CakePHP 5.x Caching](https://book.cakephp.org/5/en/core-libraries/caching.html)


[license-mit]: https://img.shields.io/badge/license-MIT-blue.svg
[packagist-badge]: https://img.shields.io/packagist/v/kba-team/cakephp-predis
[packagist]: https://packagist.org/packages/kba-team/cakephp-predis
