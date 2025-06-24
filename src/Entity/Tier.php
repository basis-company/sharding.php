<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Database;
use Basis\Sharding\Schema\UniqueIndex;

#[Caching]
class Tier implements Bootstrap, Segment, Indexing
{
    public const TABLE = 'sharding_tier';
    public const BUCKET = 'sharding_core';

    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function bootstrap(Database $database): void
    {
        $database->create(self::class, [
            'id' => 1,
            'name' => 'default',
        ]);
    }

    public static function getDomain(): string
    {
        return 'sharding';
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name']),
        ];
    }

    public static function getSegment(): string
    {
        return 'core';
    }
}
