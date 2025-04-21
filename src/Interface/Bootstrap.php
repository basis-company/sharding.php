<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Database;

interface Bootstrap
{
    public static function bootstrap(Database $database): void;
}
