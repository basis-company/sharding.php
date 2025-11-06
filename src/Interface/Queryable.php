<?php

namespace Basis\Sharding\Interface;

interface Queryable
{
    public function query(string $query, array $params = []): array|object|null;
}
