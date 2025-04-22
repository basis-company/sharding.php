<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Database;
use Basis\Sharded\Interface\Database as DatabaseInterface;
use Basis\Sharded\Schema\Segment;

interface Driver extends DatabaseInterface
{
    public function reset(): self;
    public function getDsn(): string;
    public function syncSchema(Segment $segment, Database $database): void;
    public function hasTable(string $table): bool;
}
