<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;
use Basis\Sharding\Select;

interface Driver extends Crud, Tracker
{
    public function reset(): self;
    public function getDsn(): string;
    public function getUsage(): int;
    public function select(string $table): Select;
    public function syncSchema(Database $database, string $segment): void;
    public function hasTable(string $table): bool;
}
