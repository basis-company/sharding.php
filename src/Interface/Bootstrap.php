<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;

interface Bootstrap
{
    public static function bootstrap(Database $database): void;
}
