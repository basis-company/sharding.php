<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Interface\Crud;

class Domain implements Crud
{
    public function __construct(
        public readonly Database $database,
        public readonly string $name,
    ) {
    }

    public function create(string $class, array $data): object
    {
        return $this->database->create($this->getClass($class), $data);

    }

    public function delete(string|object $class, array|int|null|string $id = null): ?object
    {
        return $this->database->delete($this->getClass($class), $id);

    }

    public function find(string $class, array $query = []): array
    {
        return $this->database->find($this->getClass($class), $query);

    }

    public function findOne(string $class, array $query): ?object
    {
        return $this->database->findOne($this->getClass($class), $query);

    }

    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        return $this->database->findOrCreate($this->getClass($class), $query, $data);
    }

    public function findOrFail(string $class, array $query): ?object
    {
        return $this->database->findOrFail($this->getClass($class), $query);
    }

    public function getClass(string|object $class): string|object
    {
        return is_string($class) ? $this->name . '.' . $class : $class;
    }

    public function select(string $table): Select
    {
        return $this->database->select($this->getClass($table));
    }

    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        return $this->database->update($this->getClass($class), $id, $data);
    }
}