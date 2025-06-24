<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Crud;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Job;
use Exception;
use Psr\Cache\CacheItemPoolInterface;
use Ramsey\Uuid\Uuid;

class Database implements Crud
{
    public readonly Locator $locator;
    private array $storages = [];
    private $context = [];

    public function __construct(
        private readonly Driver $driver,
        public readonly Schema $schema = new Schema(),
        public readonly Factory $factory = new Factory(),
        public readonly ?CacheItemPoolInterface $cache = null,
    ) {
        $this->locator = new Locator($this);

        if (!$driver->hasTable($this->schema->getClassTable(Bucket::class))) {
            foreach (array_keys(Bucket::KEYS) as $segment) {
                $driver->syncSchema($this, new Bucket(0, $segment, 0, 0, 0, 1, 0));
            }
        }

        $driver->setContext($this->getContext(...));
    }

    public function cache(string $method, array $arguments, callable $callback)
    {
        if ($this->cache) {
            $model = $this->schema->getClassModel($arguments[0]);
            if ($model && $cache = $model->getCache()) {
                $item = $this->cache->getItem(sha1($method . json_encode($arguments)));
                if ($item->isHit()) {
                    $result = $item->get();
                } else {
                    $result = $callback();
                    $item->set($result);
                    $item->expiresAfter($cache->getLifetime());
                    $this->cache->save($item);
                }
                return $result;
            }
        }

        return $callback();
    }

    public function create(string $class, array $data): object
    {
        if (property_exists($class, 'id') && (!array_key_exists('id', $data) || !$data['id'])) {
            $data['id'] = $this->generateId($class);
        }

        if ($class == Storage::class) {
            $this->storages = [];
        }

        return $this->fetchOne($class)
            ->from($data, writable: true, multiple: false)
            ->using(function (Driver $driver, string $table) use ($data) {
                return [$driver->create($table, $data)];
            });
    }

    public function delete(string|object $class, array|int|null|string $id = null): ?object
    {
        if (is_object($class)) {
            [$class, $id] = [get_class($class), $class->id];
        }

        return $this->fetchOne($class)
            ->from(['id' => $id], writable: true, multiple: true)
            ->using(function (Driver $driver, string $table) use ($id) {
                $tuple = $driver->delete($table, $id);
                return $tuple ? [$tuple] : [];
            });
    }

    public function dispatch(Job $job)
    {
        return $job($this);
    }

    public function fetch(?string $class = null): Fetch
    {
        return new Fetch($this, $class);
    }

    public function fetchOne(?string $class = null): Fetch
    {
        return $this->fetch($class)->first();
    }

    public function find(string $class, array $data = []): array
    {
        return $this->cache(__METHOD__, func_get_args(), fn() => $this->fetch($class)
            ->from($data)
            ->using(fn(Driver $driver, string $table) => $driver->find($table, $data)));
    }

    public function findOne(string $class, array $query): ?object
    {
        return $this->cache(__METHOD__, func_get_args(), fn() => $this->fetchOne($class)
            ->from($query)
            ->using(function (Driver $driver, string $table) use ($query) {
                return $driver->findOne($table, $query) ? [$driver->findOne($table, $query)] : [];
            }));
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        $instance = $this->findOne($class, $query);
        if ($instance) {
            return $instance;
        }

        if (property_exists($class, 'id') && (!array_key_exists('id', $data) || !$data['id'])) {
            $data = array_merge($data, ['id' => $this->generateId($class)]);
        }

        return $this->fetchOne($class)
            ->from($data, writable: true, multiple: false)
            ->using(function (Driver $driver, string $table) use ($query, $data) {
                return [$driver->findOrCreate($table, $query, $data)];
            });
    }

    public function findOrFail(string $class, array $query): ?object
    {
        return $this->findOne($class, $query) ?: throw new Exception('No ' . $class . ' found');
    }

    public function generateId(string $class): int|string
    {
        $model = $this->schema->getClassModel($class);

        return match ($model->getProperties()[0]->type) {
            'int' => Sequence::getNext($this, $model->table),
            'string' => Uuid::uuid4()->toString(),
            default => throw new Exception("Unsupported id type " . $model->table),
        };
    }

    public function getBuckets(string $class, array $data = [], bool $writable = false, bool $multiple = true): array
    {
        return $this->locator->getBuckets($class, $data, $writable, $multiple);
    }

    public function getContext(): array
    {
        return (is_callable($this->context) ? ($this->context)() : $this->context) ?: [];
    }

    public function getCoreDriver(): Driver
    {
        return $this->driver;
    }

    public function getDomain(string $domain): Domain
    {
        return new Domain($this, $domain);
    }

    public function getStorage(int $storageId): Storage
    {
        if (!count($this->storages)) {
            foreach ($this->find(Storage::class) as $storage) {
                $this->storages[$storage->id] = $storage;
                if ($storage->hasDriver()) {
                    continue;
                }
                if ($storage->id == 1) {
                    $storage->setDriver($this->driver);
                }
                $storage->getDriver()->setContext($this->getContext(...));
            }
        }

        if (!array_key_exists($storageId, $this->storages)) {
            throw new Exception("Storage $storageId not found");
        }

        return $this->storages[$storageId];
    }

    public function query(array|string $buckets, array $data = []): Query
    {
        if (is_string($buckets)) {
            $buckets = $this->getBuckets($buckets, $data);
        }

        return new Query($this, $buckets);
    }

    public function setContext(array|callable $context): void
    {
        $this->context = $context;
    }

    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        $instance = null;
        if (is_object($class)) {
            $instance = $class;
            [$class, $id, $data] = [get_class($class), $class->id, $id];
        }

        $next = $this->fetchOne($class)
            ->from(['id' => $id], writable: true, multiple: true)
            ->using(function (Driver $driver, string $table) use ($id, $data) {
                $row = $driver->update($table, $id, $data);
                return $row ? [$row] : [];
            });

        if ($instance) {
            foreach ($next as $k => $v) {
                $instance->$k = $v;
            }
            return $instance;
        }

        return $next;
    }
}
