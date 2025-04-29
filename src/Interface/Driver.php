<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Database;
use Basis\Sharded\Schema\Segment;

interface Driver extends Crud
{
    public function reset(): self;
    public function getDsn(): string;
    public function getUsage(): int;
    public function syncSchema(Database $database, string $segment): void;
    public function hasTable(string $table): bool;
}
