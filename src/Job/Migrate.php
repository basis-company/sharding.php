<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Migration;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Interface\Locator;
use Exception;

class Migrate implements Job
{
    public function __construct(
        public readonly string $class,
        public readonly int $pageSize = 10_000,
        public readonly int $iterations = 10_000,
    ) {
    }

    public function __invoke(Database $database)
    {
        $topologies = $database->find(Topology::class, [
            'name' => $database->schema->getClassSegment($this->class)->fullname,
        ]);

        if (count($topologies) < 2) {
            throw new Exception("Not enough topologies");
        }

        $nextTopology = array_pop($topologies);
        if ($nextTopology->status !== Topology::DRAFT_STATUS) {
            throw new Exception("Last topology is not draft");
        }

        $currentTopology = array_pop($topologies);
        $currentBuckets = $database->find(Bucket::class, [
            'name' => $currentTopology->name,
            'version' => $currentTopology->version,
            'replica' => 0,
        ]);

        $segment = $database->schema->getSegmentByName($currentTopology->name);
        $classes = $segment->getClasses();
        $tables = $segment->getTables();

        $locator = null;
        foreach ($classes as $class) {
            if (is_a($class, Locator::class, true)) {
                $locator = $class;
                break;
            }
        }

        $nextBuckets = $database->locator->generateBuckets($nextTopology);
        array_map(fn ($bucket) => $database->locator->assignStorage($bucket, $locator), $nextBuckets);
        if ($nextTopology->replicas) {
            array_map(fn ($bucket) => $database->locator->assignStorage($bucket, $locator), $nextBuckets);
        }

        foreach ($currentBuckets as $currentBucket) {
            foreach ($tables as $table) {
                $database->getStorageDriver($currentBucket->storage)->registerChanges($table, 'migration');
            }
        }

        if (!$database->schema->getClassSegment(Migration::class)) {
            $database->schema->register(Migration::class);
        }

        $migration = $database->findOrCreate(Migration::class, [
            'name' => $nextTopology->name,
            'version' => $nextTopology->version,
        ]);

        assert($migration instanceof Migration);
        $complete = 0;

        // migrate data from current to next

        while (true) {
            $bucket = $currentBuckets[$migration->bucket];
            $class = $classes[$migration->table];
            $table = $tables[$migration->table];
            $driver = $database->getStorageDriver($bucket->storage);

            $rows = $driver->select($table)
                ->where('id')
                ->isGreaterThan($migration->key)
                ->limit($this->pageSize)
                ->toArray();

            if (!count($rows)) {
                if (array_key_exists($migration->table + 1, $classes)) {
                    // next class
                    $migration = $database->update($migration, [
                        'table' => $migration->table + 1,
                        'key' => "",
                    ]);
                } elseif (array_key_exists($migration->bucket + 1, $currentBuckets)) {
                    // next bucket
                    $migration = $database->update($migration, [
                        'bucket' => $migration->bucket + 1,
                        'table' => 0,
                        'key' => "",
                    ]);
                } else {
                    // migration complete
                    break;
                }
                continue;
            }

            $sharded = [];
            $key = null;
            foreach ($rows as $row) {
                $row = (array) $row;
                $shard = $database->locator->getShard($nextTopology, $class, $row);
                if (!array_key_exists($shard, $sharded)) {
                    $sharded[$shard] = [];
                }
                $sharded[$shard][] = $row;
                $key = $row['id'];
            }

            foreach ($nextBuckets as $nextBucket) {
                if (array_key_exists($nextBucket->shard, $sharded)) {
                    $driver = $database->getStorageDriver($nextBucket->storage);
                    foreach ($sharded[$nextBucket->shard] as $row) {
                        $driver->findOrCreate($table, ['id' => $row['id']], $row);
                    }
                }
            }

            $migration = $database->update($migration, ['key' => (string) $key]);

            if (++$complete >= $this->iterations) {
                return;
            }
        }


        foreach ($currentBuckets as $currentBucket) {
            $changes = $database->getStorageDriver($currentBucket->storage)->getChanges('migration');
            if (count($changes)) {
                $sharded = [];
                foreach ($changes as $change) {
                    $class = $classes[array_search($change->table, $tables)];
                    $row = (array) $change->data;
                    $shard = $database->locator->getShard($nextTopology, $class, $row);
                    if (!array_key_exists($shard, $sharded)) {
                        $sharded[$shard] = [];
                    }
                    if (!array_key_exists($change->table, $sharded[$shard])) {
                        $sharded[$shard][$change->table] = [];
                    }

                    $sharded[$shard][$change->table][] = $change->data;
                }

                foreach ($nextBuckets as $nextBucket) {
                    if (array_key_exists($nextBucket->shard, $sharded)) {
                        $driver = $database->getStorageDriver($nextBucket->storage);
                        foreach ($sharded[$nextBucket->shard] as $table => $rows) {
                            foreach ($rows as $row) {
                                $result = $driver->update($table, $row['id'], $row);
                            }
                        }
                    }
                }

                $database->getStorageDriver($currentBucket->storage)->ackChanges($changes);
                if (++$complete >= $this->iterations) {
                    return;
                }
            }
        }

        $database->update($nextTopology, ['status' => Topology::READY_STATUS]);
        $database->update($currentTopology, ['status' => Topology::STALE_STATUS]);
    }
}
