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
            'postgresql' => [
                new Doctrine(implode('', [
                    'pdo-pgsql://',
                    getenv('POSTGRES_USER'),
                    ':',
                    getenv('POSTGRES_PASSWORD'),
                    '@',
                    getenv('POSTGRES_HOST'),
                    ':',
                    getenv('POSTGRES_PORT') ?: 3306,
                    '/',
                    getenv('POSTGRES_DATABASE'),
                ])),
            ],
            'runtime' => [new Runtime()],
            'tarantool' => [new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"))],
        ];
    }
}
