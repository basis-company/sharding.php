<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Sequence;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Crud;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Schema\Model;
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

        if (!$driver->hasTable($this->schema->getTable(Bucket::class))) {
            foreach (array_keys(Bucket::KEYS) as $segment) {
                $driver->syncSchema($this, new Bucket(0, $segment, 0, 0, 0, 1, 0));
            }
        }

        $driver->setContext($this->getContext(...));

        $factory->afterCreate(function ($instance) {
            if (method_exists($instance, 'setShardingDatabase')) {
                $instance->setShardingDatabase($this);
            }
        });
    }

    public function create(string|Model $class, array $data): object
    {
        $model = $this->schema->getModel($class);
        if ((!class_exists($class, false) || property_exists($class, 'id')) && (!array_key_exists('id', $data) || !$data['id'])) {
            $data['id'] = $this->generateId($model);
        }

        foreach ($model->getDefaults() as $k => $v) {
            if (!array_key_exists($k, $data)) {
                $data[$k] = $v;
            }
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

    public function fetch(string|Model|null $class = null): Fetch
    {
        return new Fetch($this, $class ? $this->schema->getModel($class) : null);
    }

    public function fetchOne(string|Model|null $class = null): Fetch
    {
        return $this->fetch($class)->first();
    }

    public function find(string|Model $class, array $data = []): array
    {
        return $this->fetch($class)
            ->from($data)
            ->setCache($this->cache, [__METHOD__, func_get_args()])
            ->using(fn(Driver $driver, string $table) => $driver->find($table, $data));
    }

    public function findOne(string|Model $class, array $query): ?object
    {
        return $this->fetchOne($class)
            ->from($query)
            ->setCache($this->cache, [__METHOD__, func_get_args()])
            ->using(function (Driver $driver, string $table) use ($query) {
                return $driver->findOne($table, $query) ? [$driver->findOne($table, $query)] : [];
            });
    }

    public function findOrCreate(string|Model $class, array $query, array $data = []): object
    {
        $model = $this->schema->getModel($class);

        $instance = $this->findOne($class, $query);
        if ($instance) {
            return $instance;
        }

        $data = array_merge($query, $data);

        if ((!class_exists($class, false) || property_exists($class, 'id')) && (!array_key_exists('id', $data) || !$data['id'])) {
            $data['id'] = $this->generateId($model);
        }

        foreach ($model->getDefaults() as $k => $v) {
            if (!array_key_exists($k, $data)) {
                $data[$k] = $v;
            }
        }

        return $this->fetchOne($model)
            ->from($data, writable: true, multiple: false)
            ->using(function (Driver $driver, string $table) use ($query, $data) {
                return [$driver->findOrCreate($table, $query, $data)];
            });
    }

    public function findOrFail(string|Model $class, array $query): ?object
    {
        return $this->findOne($class, $query) ?: throw new Exception('No ' . $class . ' found');
    }

    public function generateId(Model $model): int|string
    {
        if (!$model->class || !count($model->getProperties())) {
            return Sequence::getNext($this, $model);
        }

        return match ($model->getProperties()[0]->type) {
            'int' => Sequence::getNext($this, $model),
            'string' => Uuid::uuid4()->toString(),
            default => throw new Exception("Unsupported id type " . $model->table),
        };
    }

    public function getBuckets(string|Model $class, array $data = [], bool $writable = false, bool $multiple = true): array
    {
        $model = $this->schema->getModel($class);
        return $this->locator->getBuckets($model, $data, $writable, $multiple);
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

    public function select(string|Model $class): Select
    {
        $class = $class instanceof Model ? $class : $this->schema->getModel($class);
        return new Select(function (Select $global) use ($class) {
            $result = [];
            foreach ($this->getBuckets($class) as $bucket) {
                $storage = $this->getStorage($bucket->storage);
                $table = $this->schema->getModel($class)->getTable($bucket, $storage);
                $select = $storage->getDriver()->select($table);
                $select->conditions = $global->conditions;
                $select->limit = $global->limit;
                $select->orderBy = $global->orderBy;
                $select->orderByAscending = $global->orderByAscending;
                foreach ($select->toArray() as $row) {
                    $result[] = $this->factory->getInstance($class, $row);
                    if (count($result) >= $global->limit) {
                        break 2;
                    }
                }
            }
            return $result;
        });
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
