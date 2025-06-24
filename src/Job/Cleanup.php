<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Interface\Job;
use Exception;

class Cleanup implements Job
{
    public function __construct(
        public readonly string $class,
    ) {
    }

    public function __invoke(Database $database)
    {
        $topology = $database->locator->getTopology($this->class);
        $segment = $database->schema->getSegmentByName($topology->name);

        $counter = 0;

        foreach ($database->find(Bucket::class, ['name' => $topology->name]) as $bucket) {
            if ($bucket->version == $topology->version) {
                continue;
            }
            $counter++;
            $storage = $database->getStorage($bucket->storage);
            foreach ($segment->getModels() as $model) {
                $table = $model->getTable($bucket, $storage);
                if ($storage->getDriver()->hasTable($table)) {
                    $storage->getDriver()->dropTable($table);
                }
            }
            $database->delete($bucket);
        }

        if (!$counter) {
            throw new Exception("No stale buckets found");
        }
    }
}