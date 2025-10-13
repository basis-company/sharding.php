<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Migration;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Interface\Locator;
use Exception;

class Upgrade implements Job
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
            'name' => $database->schema->getModel($this->class)->segment,
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

        $models = $database->schema->getModels($currentTopology->name);
        $classes = array_map(fn ($model) => $model->class, $models);
        $tables = array_map(fn ($model) => $model->table, $models);

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
                $database->getStorage($currentBucket->storage)->getDriver()->registerChanges($table, 'migration');
            }
        }

        if (!$database->schema->getModel(Migration::class)) {
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
            $driver = $database->getStorage($bucket->storage)->getDriver();

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
                    continue;
                } elseif (array_key_exists($migration->bucket + 1, $currentBuckets)) {
                    // next bucket
                    $migration = $database->update($migration, [
                        'bucket' => $migration->bucket + 1,
                        'table' => 0,
                        'key' => "",
                    ]);
                    continue;
                } else {
                    // migration complete
                    break;
                }
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
                    $database->getStorage($nextBucket->storage)->getDriver()->insert($table, $sharded[$nextBucket->shard]);
                }
            }

            $migration = $database->update($migration, ['key' => (string) $key]);

            if (++$complete >= $this->iterations) {
                return;
            }
        }


        foreach ($currentBuckets as $currentBucket) {
            $changes = $database->getStorage($currentBucket->storage)->getDriver()->getChanges('migration');
            if (count($changes)) {
                $sharded = [];
                foreach ($changes as $change) {
                    $class = $classes[array_search($change->tablename, $tables)];
                    $row = (array) $change->data;
                    $shard = $database->locator->getShard($nextTopology, $class, $row);
                    if (!array_key_exists($shard, $sharded)) {
                        $sharded[$shard] = [];
                    }
                    if (!array_key_exists($change->tablename, $sharded[$shard])) {
                        $sharded[$shard][$change->tablename] = [];
                    }

                    $sharded[$shard][$change->tablename][] = $change->data;
                }

                foreach ($nextBuckets as $nextBucket) {
                    if (array_key_exists($nextBucket->shard, $sharded)) {
                        $driver = $database->getStorage($nextBucket->storage)->getDriver();
                        foreach ($sharded[$nextBucket->shard] as $table => $rows) {
                            foreach ($rows as $row) {
                                $driver->update($table, $row['id'], $row);
                            }
                        }
                    }
                }

                $database->getStorage($currentBucket->storage)->getDriver()->ackChanges($changes);
                if (++$complete >= $this->iterations) {
                    return;
                }
            }
        }

        $database->update($nextTopology, ['status' => Topology::READY_STATUS]);
        $database->update($currentTopology, ['status' => Topology::STALE_STATUS]);
    }
}
