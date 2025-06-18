<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Caching;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Database;
use Basis\Sharding\Schema\UniqueIndex;

#[Caching]
class Bucket implements Bootstrap, Segment, Indexing
{
    public const TABLE = 'sharding_bucket';
    public const BUCKET = 'sharding_core';

    public const KEYS = [
        Bucket::BUCKET => 1,
        Sequence::BUCKET => 2,
    ];

    public function __construct(
        public int $id,
        public string $name,
        public int $version,
        public int $shard,
        public int $replica,
        public int $storage,
    ) {
    }

    public static function bootstrap(Database $database): void
    {
        foreach (self::KEYS as $name => $id) {
            $database->getCoreDriver()->create($database->schema->getClassTable(self::class), [
                'id' => $id,
                'name' => $name,
                'storage' => 1,
            ]);
        }
    }

    public static function getDomain(): string
    {
        return 'sharding';
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name', 'version', 'shard', 'replica']),
        ];
    }

    public static function getSegment(): string
    {
        return 'core';
    }

    public function isCore(): bool
    {
        return $this->name === self::BUCKET || $this->name === Sequence::BUCKET;
    }
}
