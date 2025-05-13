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
use Ramsey\Uuid\Uuid;

class Database implements Crud
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
            array_map(fn ($segment) => $driver->syncSchema($this, $segment), array_keys(Bucket::KEYS));
        }
    }

    public function create(string $class, array $data): object
    {
        if (property_exists($class, 'id') && (!array_key_exists('id', $data) || !$data['id'])) {
            $data['id'] = $this->generateId($class);
        }

        return $this->fetchOne($class)
            ->from($data, writable: true, multiple: false)
            ->using(function (Driver $driver, string $table) use ($data) {
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
            ->using(function (Driver $driver, string $table) use ($query) {
                return $driver->findOne($table, $query) ? [$driver->findOne($table, $query)] : [];
            });
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

    public function getBuckets(string $class, array $data = [], bool $writable = false, bool $multiple = true): array
    {
        return $this->locator->getBuckets($class, $data, $writable, $multiple);
    }

    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        if (is_object($class)) {
            [$class, $id, $data] = [get_class($class), $class->id, $id];
        }
        return $this->fetchOne($class)
            ->from(['id' => $id], writable: true, multiple: true)
            ->using(function (Driver $driver, string $table) use ($id, $data) {
                $row = $driver->update($table, $id, $data);
                return $row ? [$row] : [];
            });
    }
}
