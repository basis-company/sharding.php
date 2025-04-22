<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Domain;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Interface\Segment;

class Storage implements Bootstrap, Domain, Segment
{
    public static $types = [
        'tarantool' => Tarantool::class,
        'runtime' => Runtime::class,
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
        return 'sharded';
    }

    public static function getSegment(): string
    {
        return 'storages';
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
