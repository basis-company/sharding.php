<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Router;
use Basis\Sharded\Schema\Segment;

interface Driver extends Database
{
    public function getDsn(): string;
    public function syncSchema(Segment $segment, Router $router): void;
    public function hasTable(string $table): bool;
}
