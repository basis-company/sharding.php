<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\UniqueIndex;
use Tarantool\Client\Schema\Operations;

class Sequence implements Bootstrap, Domain, Segment, Indexing
{
    public function __construct(
        public int $id,
        public string $name,
        public int $next,
    ) {
    }

    public static function bootstrap(Database $database): void
    {
        $database->create(self::class, [
            'id' => 1,
            'name' => $database->schema->getClassTable(Sequence::class),
            'next' => 1,
        ]);
        $database->create(self::class, [
            'name' => $database->schema->getClassTable(Bucket::class),
            'next' => 3,
        ]);
        $database->create(self::class, [
            'name' => $database->schema->getClassTable(Storage::class),
            'next' => 1,
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
        return 'sequences';
    }

    public static function getNext(Database $database, string $name): int
    {
        $sequence = $database->findOrCreate(self::class, [
            'name' => $name
        ], [
            'name' => $name,
            'next' => 0,
        ]);

        [$bucket] = $database->getBuckets(Sequence::class, writable: true);
        $driver = $database->getStorageDriver($bucket->storage);

        if ($driver instanceof Tarantool) {
            $driver->getMapper()->update(
                $database->schema->getClassTable(Sequence::class),
                $sequence,
                Operations::add('next', 1)
            );
        } elseif ($driver instanceof Runtime) {
            $driver->update($database->schema->getClassTable(Sequence::class), $sequence->id, [
                'next' => ++$sequence->next
            ]);
        } else {
            throw new \Exception('Unsupported driver');
        }

        return $sequence->next;
    }
}
