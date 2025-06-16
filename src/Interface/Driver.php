<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Select;

interface Driver extends Crud, Tracker
{
    public function getDsn(): string;
    public function getUsage(): int;
    public function hasTable(string $table): bool;
    public function insert(string $table, array $rows): array;
    public function query(string $query, array $params = []): array;
    public function reset(): self;
    public function select(string $table): Select;
    public function syncSchema(Database $database, Bucket $bucket): void;
}
