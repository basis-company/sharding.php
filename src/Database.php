<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Sequence;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Database as DatabaseInterface;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Interface\Job;
use Exception;
use Ramsey\Uuid\Uuid;

class Database implements DatabaseInterface
{
    public readonly Locator $locator;
    public readonly Schema $schema;
    private array $drivers = [];

    public function __construct(
        public readonly Driver $driver,
        ?Schema $schema = null,
    ) {
        $this->schema = $schema ?? new Schema();
        $this->locator = new Locator($this);

        if (!$driver->hasTable($this->schema->getClassTable(Bucket::class))) {
            $segments = array_map($this->schema->getSegmentByName(...), array_keys(Bucket::KEYS));
            array_walk($segments, fn ($segment) => $driver->syncSchema($segment, $this));
        }
    }

    public function create(string $class, array $data): object
    {
        return $this->fetchOne($class)
            ->from($data, create: true, single: true)
            ->using(function (Driver $driver, string $table) use ($class, $data) {
                if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                    $data['id'] = $this->generateId($class);
                }
                return [$driver->create($table, $data)];
            });
    }

    public function createInstance(string $class, array|object $row): object
    {
        if (is_object($row)) {
            $row = get_object_vars($row);
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

    public function dispatch(Job $job)
    {
        return $job($this);
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
            ->from($query)
            ->using(fn(Driver $driver, string $table) => [$driver->findOne($table, $query)]);
    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        return $this->fetchOne($class)
            ->from($data, create: true, single: true)
            ->using(function (Driver $driver, string $table) use ($class, $query, $data) {
                if (array_key_exists('id', $query)) {
                    $row = $driver->findOrCreate($table, $query, $data);
                } else {
                    $row = $driver->findOne($table, $query);
                    if (!$row) {
                        if (property_exists($class, 'id') && !array_key_exists('id', $data)) {
                            $data = array_merge(['id' => $this->generateId($class)], $data);
                        }
                        $row = $driver->findOrCreate($table, $query, array_merge($data));
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
                $this->drivers[$storage->id] = $storageId == 1 ? $this->driver : $storage->createDriver();
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
