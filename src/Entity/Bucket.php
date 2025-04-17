<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Interface\Subdomain;
use Basis\Sharded\Router;
use Basis\Sharded\Schema\UniqueIndex;

class Bucket implements Bootstrap, Subdomain, Indexing
{
    public const DROP_PREFIX_FLAG = 1;

    public const BUCKET_BUCKET_ID = 1;
    public const BUCKET_BUCKET_NAME = 'sharded_buckets';

    public const STORAGE_BUCKET_ID = 2;
    public const STORAGE_BUCKET_NAME = 'sharded_storages';

    public const SEQUENCE_BUCKET_ID = 3;
    public const SEQUENCE_BUCKET_NAME = 'sharded_sequences';

    public const KEYS = [
        self::BUCKET_BUCKET_NAME => self::BUCKET_BUCKET_ID,
        self::STORAGE_BUCKET_NAME => self::STORAGE_BUCKET_ID,
        self::SEQUENCE_BUCKET_NAME => self::SEQUENCE_BUCKET_ID,
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

    public static function bootstrap(Router $router): void
    {
        $router->driver->create($router->registry->getTable(self::class), [
            'bucket' => Bucket::BUCKET_BUCKET_ID,
            'id' => Bucket::BUCKET_BUCKET_ID,
            'name' => Bucket::BUCKET_BUCKET_NAME,
            'storage' => 1,
        ]);

        $router->driver->create($router->registry->getTable(self::class), [
            'bucket' => Bucket::BUCKET_BUCKET_ID,
            'id' => Bucket::STORAGE_BUCKET_ID,
            'name' => Bucket::STORAGE_BUCKET_NAME,
            'storage' => 1,
        ]);

        $router->driver->create($router->registry->getTable(self::class), [
            'bucket' => Bucket::BUCKET_BUCKET_ID,
            'id' => Bucket::SEQUENCE_BUCKET_ID,
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

    public static function getSubdomain(): string
    {
        return 'buckets';
    }

    public static function initialize(Router $router): void
    {
        $schemas = array_map($router->registry->getSchema(...), array_keys(self::KEYS));
        array_walk($schemas, fn ($schema) => $router->driver->syncSchema($schema, $router));
    }
}
