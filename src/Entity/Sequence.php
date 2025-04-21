<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Driver\Tarantool;
use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Domain;
use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Interface\Segment;
use Basis\Sharded\Router;
use Basis\Sharded\Schema\UniqueIndex;
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
            'name' => $database->meta->getClassTable(Sequence::class),
            'next' => 1,
        ]);
        $database->create(self::class, [
            'name' => $database->meta->getClassTable(Bucket::class),
            'next' => 3,
        ]);
        $database->create(self::class, [
            'name' => $database->meta->getClassTable(Storage::class),
            'next' => 1,
        ]);
    }

    public static function getDomain(): string
    {
        return 'sharded';
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
            'bucket' => Bucket::SEQUENCE_BUCKET_ID,
            'name' => $name,
            'next' => 0,
        ]);

        [$bucket] = $database->locate(Sequence::class, create: true);
        $driver = $database->getStorageDriver($bucket->storage);

        if ($driver instanceof Tarantool) {
            $driver->getMapper()->update(
                $database->meta->getClassTable(Sequence::class),
                $sequence,
                Operations::add('next', 1)
            );
        } elseif ($driver instanceof Runtime) {
            $driver->update($database->meta->getClassTable(Sequence::class), $sequence->id, [
                'next' => ++$sequence->next
            ]);
        } else {
            throw new \Exception('Unsupported driver');
        }

        return $sequence->next;
    }
}
