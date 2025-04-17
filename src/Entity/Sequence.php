<?php

namespace Basis\Sharded\Entity;

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

    public static function bootstrap(Router $router): void
    {
        $router->create(self::class, [
            'id' => 1,
            'name' => $router->meta->getTable(Sequence::class),
            'next' => 1,
        ]);
        $router->create(self::class, [
            'name' => $router->meta->getTable(Bucket::class),
            'next' => 3,
        ]);
        $router->create(self::class, [
            'name' => $router->meta->getTable(Storage::class),
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

    public static function getNext(Router $router, string $name): int
    {
        $sequence = $router->findOrCreate(self::class, [
            'name' => $name
        ], [
            'bucket' => Bucket::SEQUENCE_BUCKET_ID,
            'name' => $name,
            'next' => 0,
        ]);

        [$bucket] = $router->getBuckets(Sequence::class, createIfNotExists: true);
        $driver = $router->getDriver($bucket->storage);

        if ($driver instanceof Tarantool) {
            $driver->getMapper()->update(
                $router->meta->getTable(Sequence::class),
                $sequence,
                Operations::add('next', 1)
            );
        } elseif ($driver instanceof Runtime) {
            $driver->update($router->meta->getTable(Sequence::class), $sequence->id, ['next' => ++$sequence->next]);
        } else {
            throw new \Exception('Unsupported driver');
        }

        return $sequence->next;
    }
}
