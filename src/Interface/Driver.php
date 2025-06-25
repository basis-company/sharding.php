<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;

interface Driver extends Crud, Tracker
{
    public function dropTable(string $table): void;
    public function getDsn(): string;
    public function getUsage(): int;
    public function hasTable(string $table): bool;
    public function insert(string $table, array $rows): array;
    public function query(string $query, array $params = []): array;
    public function reset(): self;
    public function syncSchema(Database $database, Bucket $bucket): void;
}
