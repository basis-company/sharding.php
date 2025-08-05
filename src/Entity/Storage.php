<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\Index;
use Exception;

#[Caching]
class Storage implements Bootstrap, Domain, Segment, Indexing
{
    public const DEDICATED_FLAG = 1;
    public const TABLE = 'sharding_storage';

    private ?Driver $driver = null;

    public static $types = [
        'doctrine' => Doctrine::class,
        'runtime' => Runtime::class,
        'tarantool' => Tarantool::class,
    ];

    public static function bootstrap(Database $database): void
    {
        $database->create(self::class, [
            'id' => 1,
            'type' => array_search(get_class($database->getCoreDriver()), self::$types),
            'dsn' => $database->getCoreDriver()->getDsn(),
            'tier' => 1,
        ]);
    }

    public static function getDomain(): string
    {
        return 'sharding';
    }
    public static function getIndexes(): array
    {
        return [
            new Index(['tier']),
            new Index(['type', 'dsn']),
        ];
    }

    public static function getSegment(): string
    {
        return 'core';
    }

    public function __construct(
        public int $id,
        public string $type,
        public string $dsn,
        public int $flags,
        public int $tier = 1,
    ) {
    }

    public function getDriver(): Driver
    {
        if (!$this->driver) {
            $this->driver = new self::$types[$this->type]($this->dsn);
        }
        return $this->driver;
    }

    public function hasDriver(): bool
    {
        return $this->driver !== null;
    }

    public function setDriver(Driver $driver): void
    {
        if ($this->hasDriver()) {
            throw new Exception('Driver already set');
        }
        $this->driver = $driver;
    }

    public function isDedicated(): bool
    {
        return boolval($this->flags & self::DEDICATED_FLAG);
    }

}
