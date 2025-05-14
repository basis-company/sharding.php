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
    public const DEDICATED_FLAG = 1;

    public const BUCKET_BUCKET_NAME = 'sharding_buckets';
    public const STORAGE_BUCKET_NAME = 'sharding_storages';
    public const SEQUENCE_BUCKET_NAME = 'sharding_sequences';

    public const KEYS = [
        self::BUCKET_BUCKET_NAME => 1,
        self::STORAGE_BUCKET_NAME => 2,
        self::SEQUENCE_BUCKET_NAME => 3,
    ];

    public function __construct(
        public int $id,
        public string $name,
        public int $version,
        public int $shard,
        public int $replica,
        public int $storage,
        public int $flags,
    ) {
    }

    public static function bootstrap(Database $database): void
    {
        $database->driver->create($database->schema->getClassTable(self::class), [
            'id' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME],
            'name' => Bucket::BUCKET_BUCKET_NAME,
            'storage' => 1,
        ]);

        $database->driver->create($database->schema->getClassTable(self::class), [
            'id' => Bucket::KEYS[Bucket::STORAGE_BUCKET_NAME],
            'name' => Bucket::STORAGE_BUCKET_NAME,
            'storage' => 1,
        ]);

        $database->driver->create($database->schema->getClassTable(self::class), [
            'id' => Bucket::KEYS[Bucket::SEQUENCE_BUCKET_NAME],
            'name' => Bucket::SEQUENCE_BUCKET_NAME,
            'storage' => 1,
        ]);
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
        return 'buckets';
    }

    public static function isDedicated(self $bucket): bool
    {
        return boolval($bucket->flags & self::DEDICATED_FLAG);
    }
}
