<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Select;

interface Crud
{
    public function create(string $class, array $data): object;
    public function delete(string|object $class, array|int|null|string $id = null): ?object;
    public function find(string $class, array $query = []): array;
    public function findOne(string $class, array $query): ?object;
    public function findOrCreate(string $class, array $query, array $data = []): object;
    public function findOrFail(string $class, array $query): ?object;
    public function select(string $table): Select;
    public function update(string|object $class, array|int|string $id, ?array $data = null): ?object;
}
