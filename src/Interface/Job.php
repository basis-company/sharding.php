<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;

interface Job
{
    public function __invoke(Database $database);
}
