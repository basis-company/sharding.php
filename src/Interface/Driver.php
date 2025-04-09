<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Router;
use Basis\Sharded\Schema\Schema;

interface Driver extends Database
{
    public function getDsn(): string;
    public function syncSchema(Schema $schema, Router $router): void;
    public function hasTable(string $table): bool;
}
