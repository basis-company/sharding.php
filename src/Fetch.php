<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Schema\Model;
use Closure;
use Exception;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\CacheItem;

class Fetch
{
    public mixed $buckets = [];
    private ?CacheItem $cache = null;
    private CacheItemPoolInterface $pool;

    public function __construct(
        public readonly Database $database,
        public string|Model|null $class = null,
        public bool $first = false,
    ) {
    }

    public function first(bool $first = true): self
    {
        $this->first = $first;
        return $this;
    }

    public function from(array $buckets, bool $writable = false, bool $multiple = true): self
    {
        if (array_is_list($buckets) && count($buckets) && $buckets[0] instanceof Bucket) {
            $this->buckets = $buckets;
        } else {
            $this->buckets = fn() => $this->database->getBuckets($this->class, $buckets, $writable, $multiple);
        }
        return $this;
    }

    public function query(string $query, array $params = []): array|object|null
    {
        return $this->cache(function () use ($query, $params) {
            if (!$this->buckets && $this->class) {
                $this->from([]);
            }
            if (is_callable($this->buckets)) {
                $this->buckets = call_user_func($this->buckets);
            }
            if (!$this->buckets) {
                throw new Exception("No buckets defined");
            }

            $result = [];
            foreach ($this->buckets as $bucket) {
                $driver = $bucket->isCore() ? $this->database->getCoreDriver() : $this->database->getStorage($bucket->storage)->getDriver();
                $bucketResult = $driver->query($query, $params);
                if (array_is_list($bucketResult) && count($bucketResult)) {
                    foreach ($bucketResult as $row) {
                        $result[] = $row;
                    }
                } else {
                    $result = array_merge($result, $bucketResult);
                }
                if ($this->first && count($result)) {
                    break;
                }
            }
            if ($this->class) {
                if (!$this->first) {
                    // query result is array of tuples
                    $result = $result[0];
                }
                $result = array_map(fn($row) => $this->database->factory->getInstance($this->class, $row), $result);
            }

            if ($this->first) {
                return count($result) ? $result[0] : null;
            }

            return $result;
        });
    }

    public function cache(callable $callback)
    {
        if ($this->cache) {
            $model = $this->database->schema->getModel($this->class);
            if ($model && $cache = $model->getCache()) {
                if ($this->cache->isHit()) {
                    $result = $this->cache->get();
                } else {
                    $result = $callback();
                    $this->cache->set($result);
                    $this->cache->expiresAfter($cache->getLifetime());
                    $this->pool->save($this->cache);
                }
                return $result;
            }
        }

        return $callback();
    }

    public function using(callable $callback): array|object|null
    {
        return $this->cache(function () use ($callback) {
            if (!$this->class) {
                throw new Exception("No class defined");
            }
            $model = $this->database->schema->getModel($this->class);

            if ($this->buckets && !is_array($this->buckets)) {
                $this->buckets = call_user_func($this->buckets);
            }
            if (!$this->buckets) {
                $this->buckets = $this->database->getBuckets($model);
            }

            $rows = [];
            foreach ($this->buckets as $bucket) {
                if (!$bucket->storage) {
                    continue;
                }
                $table = $model->table;
                if ($bucket->isCore()) {
                    $driver = $this->database->getCoreDriver();
                } else {
                    $storage = $this->database->getStorage($bucket->storage);
                    if ($storage->isDedicated()) {
                        $table = explode('_', $table, 2)[1];
                    }
                    $driver = $storage->getDriver();
                }
                foreach ($callback($driver, $table) as $row) {
                    $rows[] = $this->database->factory->getInstance($model->class, $row);
                    if ($this->first) {
                        return array_pop($rows);
                    }
                }
            }

            return $this->first ? null : $rows;
        });
    }

    public function setCache(?CacheItemPoolInterface $pool, array $params): self
    {
        if ($pool) {
            $this->cache = $pool->getItem(sha1(serialize($params)));
            $this->pool = $pool;
        }

        return $this;
    }
}
