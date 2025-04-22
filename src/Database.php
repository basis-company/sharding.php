<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Database as DatabaseInterface;
use Basis\Sharded\Interface\Driver;
use Exception;
use Ramsey\Uuid\Uuid;

class Database implements DatabaseInterface
{
    public readonly Locator $locator;
    private array $drivers = [];

    public function __construct(
        public readonly Schema $schema,
        public readonly Driver $driver,
    ) {
        $this->locator = new Locator($this);

        if (!$driver->hasTable($schema->getClassTable(Bucket::class))) {
            $segments = array_map($schema->getSegmentByName(...), array_keys(Bucket::KEYS));
            array_walk($segments, fn ($segment) => $driver->syncSchema($segment, $this));
        }
    }

    public function create(string $class, array $data): object
    {
        return $this->fetchOne($class)
            ->from($data, create: true, single: true)
            ->using(function (Driver $driver, string $table, array $buckets) use ($class, $data) {
                if (!Bucket::isDedicated($buckets[0])) {
                    $data['bucket'] = $buckets[0]->id;
                }
                if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                    $data['id'] = $this->generateId($class);
                }
                return [$driver->create($table, $data)];
            });
    }

    public function createInstance(string $class, array|object $row, bool $isDedicated = false): object
    {
        if (is_object($row)) {
            $row = get_object_vars($row);
        }
        if (!$isDedicated) {
            array_shift($row);
        }
        if ($class && class_exists($class)) {
            return new $class(...array_values($row));
        }
        return (object) $row;
    }

    public function delete(string|object $class, ?int $id = null): ?object
    {
        if (is_object($class)) {
            [$class, $id] = [get_class($class), $class->id];
        }

        return $this->fetchOne($class)
            ->from(['id' => $id])
            ->using(fn (Driver $driver, string $table) => [$driver->delete($table, $id)]);
    }

    public function fetch(string $class): Fetch
    {
        return new Fetch($this, $class);
    }

    public function fetchOne(string $class): Fetch
    {
        return $this->fetch($class)->first();
    }

    public function find(string $class, array $data = []): array
    {
        return $this->fetch($class)
            ->from($data)
            ->using(fn(Driver $driver, string $table) => $driver->find($table, $data));
    }

    public function findOne(string $class, array $query): ?object
    {
        return $this->fetchOne($class)
            ->from($query, single: true)
            ->using(fn(Driver $driver, string $table) => [$driver->findOne($table, $query)]);
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        return $this->fetchOne($class)
            ->from($data, create: true, single: true)
            ->using(function (Driver $driver, string $table, array $buckets) use ($class, $query, $data) {
                if (array_key_exists('id', $data)) {
                    $row = $driver->findOrCreate($table, $query, $data);
                } else {
                    $row = $driver->findOne($table, $query);
                    if (!$row) {
                        if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                            $extra['id'] = $this->generateId($class);
                        }
                        if (!Bucket::isDedicated($buckets[0])) {
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

    public function locate(string $class, array $data = [], bool $create = false, bool $single = false): array
    {
        return $this->locator->getBuckets($class, $data, $create, $single);
    }

    public function update(string|object $class, int|array $id, ?array $data = null): ?object
    {
        if (is_object($class)) {
            [$class, $id, $data] = [get_class($class), $class->id, $id];
        }
        return $this->fetchOne($class)
            ->from($data, single: true)
            ->using(fn (Driver $driver, string $table) => [$driver->update($table, $id, $data)]);
    }
}
