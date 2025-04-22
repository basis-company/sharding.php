<?php

namespace Basis\Sharded\Interface;

interface Database
{
    public function create(string $class, array $data): object;
    public function delete(string $class, int $id): ?object;
    public function find(string $class, array $query = []): array;
    public function findOne(string $class, array $query): ?object;
    public function findOrCreate(string $class, array $query, array $data = []): object;
    public function findOrFail(string $class, array $query): ?object;
    public function update(string $class, int $id, array $data): ?object;
}
