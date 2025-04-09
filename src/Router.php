<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Database;
use Basis\Sharded\Interface\Driver;
use Exception;

class Router implements Database
{

    public function __construct(
        public readonly Registry $registry,
        public readonly Driver $driver,
    ) {
    }

    public function castStorage(Bucket $bucket): void
    {
        if ($bucket->storage) {
            return;
        }

        $storages = $this->find(Storage::class);
        if (count($storages) !== 1) {
            throw new Exception('No storage casting');
        }

        [$storage] = $storages;

        $driver = $this->getDriver($storage->id);
        $driver->update($this->registry->getTable(Bucket::class), $bucket->id, [
            'storage' => $storage->id,
        ]);

        $bucket->storage = $storage->id;
        $driver->syncSchema($this->registry->getSchema($bucket->name), $this);
    }

    public function create(string $class, array $data): object
    {
        if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
            $data['id'] = Sequence::getNext($this, $this->registry->getTable($class));
        }

        $bucket = $this->getBucket($class, $data);
        $data['bucket'] = $bucket->id;

        if (!$bucket->storage) {
            $this->castStorage($bucket);
        }

        $row = $this->perform($bucket->storage, [$bucket], $class, function (Context $context) use ($data) {
            return $context->driver->create($context->table, $data);
        });

        return $this->createInstance($class, $row);
    }

    public function createInstance(string $class, array|object $row): object
    {
        if (is_object($row)) {
            $row = get_object_vars($row);
        }
        array_shift($row);
        if (!class_exists($class)) {
            $class = $this->registry->getClass($class);
        }
        if ($class && class_exists($class)) {
            return new $class(...array_values($row));
        }
        return (object) $row;
    }

    public function createInstances(string $class, array $rows): array
    {
        return array_map(fn ($row) => $this->createInstance($class, $row), $rows);
    }

    public function find(string $class, array $data = []): array
    {
        return $this->merge($class, $data, function (Context $context) use ($data) {
            return $context->driver->find($context->table, $data);
        });
    }

    public function findOne(string $class, array $query): ?object
    {
        $data = $this->merge($class, $query, function (Context $context) use ($query) {
            return $context->driver->findOne($context->table, $query);
        });

        if (count($data)) {
            return array_shift($data);
        }

        return null;
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        $bucket = $this->getBucket($class, $query);
        $driver = $this->getDriver($bucket->storage);
        $table = $this->registry->getTable($class);

        if (array_key_exists('id', $data)) {
            $row = $driver->findOrCreate($table, $query, $data);
        } else {
            $row = $driver->findOne($table, $query);
            if (!$row) {
                $id = Sequence::getNext($this, $table);
                $row = $driver->findOrCreate($table, $query, array_merge($data, compact('id')));
            }
        }

        return $row ? $this->createInstance($class, $row) : null;
    }

    public function get(string $class, int $id): object
    {
        return (object) [];
    }

    public function getBucket(string $class, array $data = []): Bucket
    {
        $buckets = $this->getBuckets($class, $data, true);
        if (count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        return $buckets[0];
    }

    public function getBuckets(string $class, array $data = [], bool $createIfNotExists = false)
    {
        if (!$this->driver->hasTable($this->registry->getTable(Bucket::class))) {
            Bucket::initialize($this);
        }

        if (in_array($class, [Bucket::class, Storage::class, Sequence::class])) {
            return $this->createInstances(Bucket::class, $this->driver->find(
                $this->registry->getTable(Bucket::class),
                ['id' => Bucket::KEYS[$this->registry->getDomain(Bucket::class)]],
            ));
        }

        $table = $this->registry->getTable($class);
        $domain = $this->registry->getDomain($table);

        $buckets = $this->driver->find($this->registry->getTable(Bucket::class), ['name' => $domain]);
        $buckets = array_map(fn ($data) => $this->createInstance(Bucket::class, $data), $buckets);

        if (count($buckets)) {
            return $buckets;
        }

        if ($createIfNotExists) {
            return [$this->create(Bucket::class, ['name' => $domain])];
        }

        return [];
    }

    private array $drivers = [];

    public function getDriver(int $storageId): Driver
    {
        if (!count($this->drivers)) {
            foreach ($this->find(Storage::class) as $storage) {
                $this->drivers[$storage->id] = $storage->getDriver();
            }
        }

        return $this->drivers[$storageId];
    }

    public function merge(string $class, array $data, callable $callback): array
    {
        $storageBuckets = [];
        foreach ($this->getBuckets($class, $data) as $bucket) {
            if (!array_key_exists($bucket->storage, $storageBuckets)) {
                $storageBuckets[$bucket->storage] = [$bucket];
            } else {
                $storageBuckets[$bucket->storage][] = $bucket;
            }
        }

        $result = [];
        foreach ($storageBuckets as $storage => $buckets) {
            foreach ($this->perform($storage, $buckets, $class, $callback) as $row) {
                $result[] = $this->createInstance($class, $row);
            }
        }

        return $result;
    }

    public function perform(int $storage, array $buckets, string $class, callable $callback)
    {
        if ($buckets[0]->name == Bucket::BUCKET_BUCKET_NAME) {
            $driver = $this->driver;
        } else {
            $driver = $this->getDriver($storage);
        }

        return $callback(new Context($driver, $this->registry->getTable($class)));
    }

    public function update(string $class, int $id, array $data): void
    {
        throw new Exception("STORAGE");
    }
}
