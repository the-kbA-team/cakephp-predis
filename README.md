# CakePHP Predis

[![License: MIT][license-mit]](LICENSE)
[![Packagist Version][packagist-badge]][packagist]

Predis plugin for CakePHP 2.x

## Usage

```bash
composer require kba-team/cakephp-predis
```

In bootstrap.php load the Plugin ...

```php
CakePlugin::load("PredisCache");
```

In your config file ...

```php
Cache::config('_session_', [
    'engine' => 'PredisCache.Predis',
    'sentinel' => ['<sentinel host 1>', ...., '<sentinel host n>'],
    'password' => "<password>",
    'port' => 26379,
    'exceptions' => true,
    'database' => 1,
    'prefix' => "",
    'duration' => '+2 days', 
]);
```

Possible configuration parameters are:
* `engine`  Predis
* `scheme`  Currently, TCP only
* `prefix`  See CakePhp 2.x Caching
* `server`   Redis server (must be master),
* `sentinel`  List of sentinel nodes (hostnames or IP addresses)
* `port`    Either redis Port (6379) is `server` is used otherwise the sentinel port (26379)
* `exceptions` Should exceptions be thrown or not (true)
* `database` See CakePhp 2.x Caching                         => 0,
* `password` Redis password
* `service`  Sentinel only: Sentinel service name (mymaster)

## Development

Use Docker and Make; no local PHP or Composer installation is required.
The supported PHP range remains `^5.5|^7.0`. Run `make help` for all targets.

```bash
make validate
make lint PHP_VERSION=5.5
make install PHP_VERSION=5.5
make install PHP_VERSION=5.5 DEPENDENCIES_LOWEST=1
make audit
make clean
```

Dependency resolution currently fails explicitly: the stock `composer:2`
image lacks the required `ext-redis`, and Composer's advisory policy blocks
the required CakePHP 2.x releases. A Redis-enabled Composer image can be
selected with `COMPOSER_IMAGE`, but the security blocker needs a separate
dependency modernization. This workflow does not ignore platform requirements
or disable advisory blocking; dependency-install CI jobs remain blocked until
these prerequisites are addressed.

Each install resolves dependencies for `PHP_VERSION`, optionally preferring
the lowest versions, then removes the temporary Composer platform configuration
and lockfile even on failure. Existing non-platform Composer settings are
preserved. Do not commit a lockfile or edit generated files under `vendor/`.
Proxy variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`) are forwarded.
Set `CA_CERT_FILE` to a PEM file when an additional CA is required.
Composer downloads are cached in `~/.cache/composer` (override with
`COMPOSER_CACHE_DIR`). Syntax checks use the actual PHP Docker runtime.
No test or static-analysis targets are supplied because no QA tools are declared.

### Further reading

* About [CakePHP 2.x Caching](https://book.cakephp.org/2/en/core-libraries/caching.html)


[license-mit]: https://img.shields.io/badge/license-MIT-blue.svg
[packagist-badge]: https://img.shields.io/packagist/v/kba-team/cakephp-predis
[packagist]: https://packagist.org/packages/kba-team/cakephp-predis
