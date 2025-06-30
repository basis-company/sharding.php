<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Bootstrap;
use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\UniqueIndex;
use Exception;
use Tarantool\Client\Schema\Operations;

class Sequence implements Bootstrap, Domain, Segment, Indexing
{
    public const TABLE = 'sharding_sequence';
    public const BUCKET = 'sharding_sequence';

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
            'next' => 2,
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

    public static function getNext(Database $database, string $table): int
    {
        $sequence = $database->findOne(self::class, ['name' => $table]);

        if (!$sequence) {
            $next = 0;
            if ($database->schema->hasTable($table)) {
                $class = $database->schema->getTableClass($table);
                $buckets = $database->locator->getBuckets($class, []);
                foreach ($buckets as $bucket) {
                    if (!$bucket->storage) {
                        continue;
                    }
                    $storage = $database->getStorage($bucket->storage);
                    $storageTable = $database->schema->getClassModel($class)->getTable($bucket, $storage);
                    $driver = $storage->getDriver();
                    if (!$driver->hasTable($storageTable)) {
                        continue;
                    }
                    if ($driver instanceof Doctrine) {
                        [$max] = $driver->query("select max(id) from $storageTable");
                        $next = max($next, $max['max'] ?: 0);
                    }
                    if ($driver instanceof Runtime) {
                        $rows = array_reverse($driver->find($storageTable));
                        if (count($rows)) {
                            $next = max($next, $rows[0]['id']);
                        }
                    }
                    if ($driver instanceof Tarantool) {
                        try {
                            $max = $driver->query("return box.space.$storageTable.index.id:max()");
                            if (count($max)) {
                                $next = max($next, $max[0][0]);
                            }
                        } catch (Exception) {}
                    }
                }

            }
            $sequence = $database->findOrCreate(self::class, [
                'name' => $table
            ], [
                'name' => $table,
                'next' => $next,
            ]);
        }

        $driver = $database->getCoreDriver();

        if ($driver instanceof Runtime) {
            $driver->update($database->schema->getClassTable(Sequence::class), $sequence->id, [
                'next' => ++$sequence->next
            ]);
        } elseif ($driver instanceof Doctrine) {
            $sequence->next = $driver->getConnection()->transactional(function () use ($driver, $database, $sequence) {
                $driver->getConnection()->executeStatement(
                    "update " . $database->schema->getClassTable(Sequence::class) . " set next = next + 1 where id = ?",
                    [$sequence->id]
                );
                return $driver->getConnection()->fetchOne(
                    "select next from " . $database->schema->getClassTable(Sequence::class) . " where id = ?",
                    [$sequence->id]
                );
            });
        } elseif ($driver instanceof Tarantool) {
            $driver->getMapper()->update(
                $database->schema->getClassTable(Sequence::class),
                $sequence,
                Operations::add('next', 1)
            );
        } else {
            throw new \Exception('Unsupported driver');
        }

        return $sequence->next;
    }

    public static function getSegment(): string
    {
        return 'sequence';
    }
}
