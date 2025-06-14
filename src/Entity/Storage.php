<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Segment;

#[Caching]
class Storage implements Bootstrap, Domain, Segment
{
    public const TABLE = 'sharding_storage';

    public static $types = [
        'runtime' => Runtime::class,
        'tarantool' => Tarantool::class,
    ];

    public static function bootstrap(Database $database): void
    {
        $database->create(self::class, [
            'id' => 1,
            'type' => array_search(get_class($database->driver), self::$types),
            'dsn' => $database->driver->getDsn(),
        ]);
    }

    public static function getDomain(): string
    {
        return 'sharding';
    }

    public static function getSegment(): string
    {
        return 'core';
    }

    public function __construct(
        public int $id,
        public string $type,
        public string $dsn,
    ) {
    }

    public function createDriver(): Driver
    {
        return new self::$types[$this->type]($this->dsn);
    }
}
