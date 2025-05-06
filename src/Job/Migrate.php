<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Interface\Job;
use Basis\Sharding\Interface\Locator;
use Exception;

class Migrate implements Job
{
    public function __construct(
        public readonly string $name,
    ) {
    }

    public function __invoke(Database $database)
    {
        $topologies = $database->find(Topology::class, [
            'name' => $this->name,
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
        $locator = null;
        foreach ($segment->getClasses() as $class) {
            if (is_a($class, Locator::class, true)) {
                $locator = $class;
                break;
            }
        }

        $nextBuckets = $database->locator->generateBuckets($nextTopology);
        array_map(fn ($bucket) => $database->locator->assignStorage($bucket, $locator), $nextBuckets);

        foreach ($currentBuckets as $currentBucket) {
            foreach ($segment->getTables() as $table) {
                $database->getStorageDriver($currentBucket->storage)->registerChanges($table, 'migration');
            }
        }

        $data = [];
        foreach ($segment->getClasses() as $class) {
            $data[$class] = [];
        }

        // migrate data from current to next
        foreach ($currentBuckets as $bucket) {
            foreach ($data as $class => $rows) {
                $driver = $database->getStorageDriver($bucket->storage);
                $data[$class] = array_merge($rows, $driver->find($segment->getTable($class)));
            }
        }

        $sharded = [];
        foreach ($data as $class => $rows) {
            $sharded[$class] = [];
            foreach ($rows as $row) {
                $shard = $database->locator->getShard($nextTopology, $class, $row);
                if (!array_key_exists($shard, $sharded[$class])) {
                    $sharded[$class][$shard][] = $row;
                }
            }
        }
        foreach ($nextBuckets as $nextBucket) {
            foreach ($sharded as $class => $shardedRows) {
                // foreach ($shardedRows[$nextBucket->shard] as $rows)
                // $driver = $database->getStorageDriver($nextBucket->storage);
                // $driver->insert($segment->getTable($class), $rows[$nextBucket->shard]);
            }
        }

        foreach ($currentBuckets as $currentBucket) {
            foreach ($segment->getTables() as $table) {
                $changes = $database->getStorageDriver($currentBucket->storage)->getChanges('migration');
                if (count($changes)) {
                    // apply migration changes
                    throw new Exception("Migration changes not applied");
                }
            }
        }


        $database->update($nextTopology, ['status' => Topology::READY_STATUS]);
        if ($nextTopology->replicas) {
            array_map(fn ($bucket) => $database->locator->assignStorage($bucket, $locator), $nextBuckets);
        }
    }
}
