<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;
use Basis\Sharding\Schema\Segment;

interface Driver extends Crud
{
    public function reset(): self;
    public function getDsn(): string;
    public function getUsage(): int;
    public function syncSchema(Database $database, string $segment): void;
    public function hasTable(string $table): bool;
}
