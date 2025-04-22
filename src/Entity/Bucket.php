<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Interface\Segment;
use Basis\Sharded\Database;
use Basis\Sharded\Schema\UniqueIndex;

class Bucket implements Bootstrap, Segment, Indexing
{
    public const DEDICATED_FLAG = 1;

    public const BUCKET_BUCKET_NAME = 'sharded_buckets';
    public const STORAGE_BUCKET_NAME = 'sharded_storages';
    public const SEQUENCE_BUCKET_NAME = 'sharded_sequences';

    public const KEYS = [
        self::BUCKET_BUCKET_NAME => 1,
        self::STORAGE_BUCKET_NAME => 2,
        self::SEQUENCE_BUCKET_NAME => 3,
    ];

    public function __construct(
        public int $id,
        public int $parent,
        public string $name,
        public int $shard,
        public int $storage,
        public int $flags,
    ) {
    }

    public static function bootstrap(Database $database): void
    {
        $database->driver->create($database->schema->getClassTable(self::class), [
            'bucket' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME],
            'id' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME],
            'name' => Bucket::BUCKET_BUCKET_NAME,
            'storage' => 1,
        ]);

        $database->driver->create($database->schema->getClassTable(self::class), [
            'bucket' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME],
            'id' => Bucket::KEYS[Bucket::STORAGE_BUCKET_NAME],
            'name' => Bucket::STORAGE_BUCKET_NAME,
            'storage' => 1,
        ]);

        $database->driver->create($database->schema->getClassTable(self::class), [
            'bucket' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME],
            'id' => Bucket::KEYS[Bucket::SEQUENCE_BUCKET_NAME],
            'name' => Bucket::SEQUENCE_BUCKET_NAME,
            'storage' => 1,
        ]);
    }

    public static function getDomain(): string
    {
        return 'sharded';
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name', 'shard', 'parent']),
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
