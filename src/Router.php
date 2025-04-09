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

        $buckets = $this->getBuckets($class, $data, createIfNotExists: true);
        if (count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        } else {
            [$bucket] = $buckets;
        }

        $data['bucket'] = $bucket->id;

        if (!$bucket->storage) {
            $this->castStorage($bucket);
        }

        $row = $this->getDriver($bucket->storage)->create($this->registry->getTable($class), $data);
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

    public function fetchInstance(string $class, array $data, callable $callback): ?object
    {
        $rows = $this->fetchInstances($class, $data, $callback, single: true);
        return count($rows) ? $rows[0] : null;
    }

    public function fetchInstances(string $class, array $data, callable $callback, bool $single = false): array
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
        $table = $this->registry->getTable($class);
        foreach ($storageBuckets as $storageId => $buckets) {
            $driver = $this->getDriver($storageId);
            foreach ($callback($driver, $table, $buckets) as $row) {
                $result[] = $this->createInstance($class, $row);
                if ($single) {
                    break 2;
                }
            }
        }

        return $result;
    }

    public function find(string $class, array $data = []): array
    {
        return $this->fetchInstances($class, $data, fn(Driver $driver, string $table) => $driver->find($table, $data));
    }

    public function findOne(string $class, array $query): ?object
    {
        return $this->fetchInstance(
            class: $class,
            data: $query,
            callback: fn(Driver $driver, string $table) => $driver->findOne($table, $query),
        );
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        $buckets = $this->getBuckets($class, $query, createIfNotExists: true);
        if (count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        } else {
            [$bucket] = $buckets;
        }

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

    public function findOrFail(string $class, array $query): ?object
    {
        $row = $this->findOne($class, $query);
        if (!$row) {
            throw new Exception('No ' . $class . ' found');
        }
        return $row;
    }

    public function getBuckets(string $class, array $data = [], bool $createIfNotExists = false)
    {
        if (!$this->driver->hasTable($this->registry->getTable(Bucket::class))) {
            Bucket::initialize($this);
        }

        if (in_array($class, [Bucket::class, Storage::class, Sequence::class])) {
            $row = $this->driver->findOrFail(
                $this->registry->getTable(Bucket::class),
                ['id' => Bucket::KEYS[$this->registry->getDomain(Bucket::class)]],
            );
            return [$this->createInstance(Bucket::class, $row)];
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
        if ($storageId == 1) {
            return $this->driver;
        }

        if (!count($this->drivers)) {
            foreach ($this->find(Storage::class) as $storage) {
                $this->drivers[$storage->id] = $storage->getDriver();
            }
        }

        return $this->drivers[$storageId];
    }

    public function update(string $class, int $id, array $data): void
    {
        $this->fetchInstance($class, $data, fn(Driver $driver, string $table) => $driver->update($table, $id, $data));
    }
}
