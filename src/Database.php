<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Database as DatabaseInterface;
use Basis\Sharded\Interface\Driver;
use Exception;

class Database implements DatabaseInterface
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

        $driver = $this->getStorageDriver($storage->id);
        $driver->update($this->meta->getClassTable(Bucket::class), $bucket->id, [
            'storage' => $storage->id,
        ]);

        $bucket->storage = $storage->id;
        $driver->syncSchema($this->meta->getSegmentByName($bucket->name), $this);
    }

    public function create(string $class, array $data): object
    {
        return $this->fetchOne($class)
            ->from($this->getBuckets($class, $data, castStorage: true, createIfNotExists: true))
            ->using(function (Driver $driver, string $table, array $buckets) use ($class, $data) {
                if (!($buckets[0]->flags & Bucket::DROP_PREFIX_FLAG)) {
                    $data['bucket'] = $buckets[0]->id;
                }
                if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                    $data['id'] = Sequence::getNext($this, $table);
                }
                return [$driver->create($table, $data)];
            });
    }

    public function createInstance(string $class, array|object $row, bool $dropBucket = true): object
    {
        if (is_object($row)) {
            $row = get_object_vars($row);
        }
        if ($dropBucket) {
            array_shift($row);
        }
        if ($class && class_exists($class)) {
            return new $class(...array_values($row));
        }
        return (object) $row;
    }

    public function fetch(string $class): Fetch
    {
        return new Fetch($this, $class);
    }

    public function fetchOne(string $class): Fetch
    {
        return new Fetch($this, $class, true);
    }

    public function fetchInstances(
        string $class,
        array $data,
        callable $callback,
        bool $castStorage = false,
        bool $createIfNotExists = true
    ): array {
        $buckets = $this->getBuckets($class, $data, castStorage: $castStorage, createIfNotExists: $createIfNotExists);

        return $this->fetch($class)->from($buckets)->using($callback);
    }

    public function find(string $class, array $data = []): array
    {
        return $this->fetch($class)
            ->from($this->getBuckets($class, $data))
            ->using(fn(Driver $driver, string $table) => $driver->find($table, $data));
    }

    public function findOne(string $class, array $query): ?object
    {
        return $this->fetchOne($class)
            ->from($this->getBuckets($class, $query, single: true))
            ->using(fn(Driver $driver, string $table) => $driver->findOne($table, $query));
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        return $this->fetchOne($class)
            ->from($this->getBuckets($class, $data, castStorage: true, createIfNotExists: true))
            ->using(function (Driver $driver, string $table, array $buckets) use ($class, $query, $data) {
                if (array_key_exists('id', $data)) {
                    $row = $driver->findOrCreate($table, $query, $data);
                } else {
                    $row = $driver->findOne($table, $query);
                    if (!$row) {
                        if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                            $extra['id'] = Sequence::getNext($this, $table);
                        }
                        if (!($buckets[0]->flags & Bucket::DROP_PREFIX_FLAG)) {
                            $extra['bucket'] = $buckets[0]->id;
                        }
                        $row = $driver->findOrCreate($table, $query, array_merge($data, $extra));
                    }
                }
                return [$row];
            });
    }

    public function findOrFail(string $class, array $query): ?object
    {
        $row = $this->findOne($class, $query);
        if (!$row) {
            throw new Exception('No ' . $class . ' found');
        }
        return $row;
    }

    public function getBuckets(
        string $class,
        array $data = [],
        bool $castStorage = false,
        bool $createIfNotExists = false,
        bool $single = false,
    ): array {
        if (!$this->driver->hasTable($this->meta->getClassTable(Bucket::class))) {
            Bucket::initialize($this);
        }

        if (in_array($class, [Bucket::class, Storage::class, Sequence::class])) {
            $row = $this->driver->findOrFail(
                $this->meta->getClassTable(Bucket::class),
                ['id' => Bucket::KEYS[$this->meta->getClassSegment(Bucket::class)->prefix]],
            );
            return [$this->createInstance(Bucket::class, $row)];
        }

        if (!class_exists($class)) {
            foreach (['.', '_'] as $candidate) {
                if (str_contains($class, $candidate)) {
                    $domain = explode($candidate, $class, 2)[0];
                    break;
                }
            }
        } else {
            $domain = $this->meta->getClassSegment($class)->prefix;
        }

        $buckets = $this->driver->find($this->meta->getClassTable(Bucket::class), ['name' => $domain]);
        $buckets = array_map(fn ($data) => $this->createInstance(Bucket::class, $data), $buckets);

        if ($single && count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        if (!count($buckets) && $createIfNotExists) {
            $buckets = [$this->create(Bucket::class, ['name' => $domain])];
        }
        if ($castStorage) {
            array_walk($buckets, $this->castStorage(...));
        }

        return $buckets;
    }

    private array $drivers = [];

    public function getStorageDriver(int $storageId): Driver
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
        return $this->fetchOne($class)
            ->from($this->getBuckets($class, $data, single: true))
            ->using(fn (Driver $driver, string $table) => $driver->update($table, $id, $data));
    }
}
