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
        public readonly Meta $meta,
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
        $driver->update($this->meta->getTable(Bucket::class), $bucket->id, [
            'storage' => $storage->id,
        ]);

        $bucket->storage = $storage->id;
        $driver->syncSchema($this->meta->getSchema($bucket->name), $this);
    }

    public function create(string $class, array $data): object
    {
        if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
            $data['id'] = Sequence::getNext($this, $this->meta->getTable($class));
        }

        $buckets = $this->getBuckets($class, $data, createIfNotExists: true);
        if (count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        } else {
            [$bucket] = $buckets;
        }

        if (!($bucket->flags & Bucket::DROP_PREFIX_FLAG)) {
            $data['bucket'] = $bucket->id;
        }

        if (!$bucket->storage) {
            $this->castStorage($bucket);
        }

        $row = $this->getDriver($bucket->storage)->create($this->meta->getTable($class), $data);
        return $this->createInstance($class, $row);
    }

    public function createInstance(string $class, array|object $row, bool $dropBucket = true): object
    {
        if (is_object($row)) {
            $row = get_object_vars($row);
        }
        if ($dropBucket) {
            array_shift($row);
        }
        if (!class_exists($class)) {
            $class = $this->meta->getClass($class);
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
            if (!$bucket->storage) {
                continue;
            }
            if (!array_key_exists($bucket->storage, $storageBuckets)) {
                $storageBuckets[$bucket->storage] = [$bucket];
            } else {
                $storageBuckets[$bucket->storage][] = $bucket;
            }
        }

        $result = [];
        foreach ($storageBuckets as $storageId => $buckets) {
            $table = $this->meta->getTable($class);
            $prefixPresent = $buckets[0]->flags & Bucket::DROP_PREFIX_FLAG;
            if ($prefixPresent) {
                [$_, $table] = explode('_', $table, 2);
            }
            $driver = $this->getDriver($storageId);
            $rows = $callback($driver, $table, $buckets);
            foreach ($rows as $row) {
                $result[] = $this->createInstance($class, $row, !$prefixPresent);
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
        $table = $this->meta->getTable($class);

        if (array_key_exists('id', $data)) {
            $row = $driver->findOrCreate($table, $query, $data);
        } else {
            $row = $driver->findOne($table, $query);
            if (!$row) {
                $row = $driver->findOrCreate($table, $query, array_merge($data, [
                    'bucket' => $bucket->id,
                    'id' => Sequence::getNext($this, $table),
                ]));
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
        if (!$this->driver->hasTable($this->meta->getTable(Bucket::class))) {
            Bucket::initialize($this);
        }

        if (in_array($class, [Bucket::class, Storage::class, Sequence::class])) {
            $row = $this->driver->findOrFail(
                $this->meta->getTable(Bucket::class),
                ['id' => Bucket::KEYS[$this->meta->getDomain(Bucket::class)]],
            );
            return [$this->createInstance(Bucket::class, $row)];
        }

        $table = $this->meta->getTable($class);
        $domain = $this->meta->getDomain($table);

        $buckets = $this->driver->find($this->meta->getTable(Bucket::class), ['name' => $domain]);
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

    public function update(string $class, int $id, array $data): ?object
    {
        return $this->fetchInstance(
            class: $class,
            data: $data,
            callback: fn (Driver $driver, string $table) => $driver->update($table, $id, $data)
        );
    }
}
