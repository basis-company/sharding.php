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
        return $this->database->create($this->name . '.' . $class, $data);

    }
    public function delete(string|object $class, array|int|null|string $id = null): ?object
    {
        if (is_string($class)) {
            $class = $this->name . '.' . $class;
        }
        return $this->database->delete($class, $id);

    }
    public function find(string $class, array $query = []): array
    {
        return $this->database->find($this->name . '.' . $class, $query);

    }
    public function findOne(string $class, array $query): ?object
    {
        return $this->database->findOne($this->name . '.' . $class, $query);

    }
    public function findOrCreate(string $class, array $query, array $data = []): object
    {
        return $this->database->findOrCreate($this->name . '.' . $class, $query, $data);

    }
    public function findOrFail(string $class, array $query): ?object
    {
        return $this->database->findOrFail($this->name . '.' . $class, $query);

    }
    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object
    {
        if (is_string($class)) {
            $class = $this->name . '.' . $class;
        }
        return $this->database->update($class, $id, $data);
    }

}