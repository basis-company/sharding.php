<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;

class TestProvider
{
    public static function drivers(): array
    {
        return [
            'runtime' => [new Runtime()],
            'tarantool' => [new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"))],
        ];
    }
}
