<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Domain;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Interface\Segment;
use Basis\Sharded\Router;

class Storage implements Bootstrap, Domain, Segment
{
    public static $types = [
        'tarantool' => Tarantool::class,
        'runtime' => Runtime::class,
    ];

    /**
     * @var Driver[]
     */
    public static array $drivers = [];

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

    public function getDriver(): Driver
    {
        if (!array_key_exists($this->id, self::$drivers)) {
            self::$drivers[$this->id] = new self::$types[$this->type]($this->dsn);
        }

        return self::$drivers[$this->id];
    }
}
