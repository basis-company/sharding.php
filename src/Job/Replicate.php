<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Job;
use Exception;

class Replicate implements Job
{
    private int $complete = 0;

    public function __construct(
        public readonly int $storage,
        public readonly int $delay = 0,
        public readonly int $limit = 0,
    ) {
    }

    public function __invoke(Database $database)
    {
        $replicas = [];
        foreach ($database->find(Topology::class) as $topology) {
            $replicas[$topology->name] = $topology->replicas;
        }

        $buckets = $database->find(Bucket::class);
        $tableDrivers = [];

        foreach ($buckets as $master) {
            if ($master->storage != $this->storage || !array_key_exists($master->name, array_filter($replicas))) {
                continue;
            }
            foreach ($buckets as $replica) {
                if ($replica->name != $master->name || $replica->shard != $master->shard || !$replica->replica) {
                    continue;
                }
                foreach ($database->schema->getModels($master->name) as $model) {
                    if (!array_key_exists($model->table, $tableDrivers)) {
                        $tableDrivers[$model->table] = [];
                    }
                    $tableDrivers[$model->table][] = $database->getStorage($replica->storage)->getDriver();
                }
            }
        }

        $storage = $database->getStorage($this->storage)->getDriver();

        while (!$this->limit || $this->complete < $this->limit) {
            $changes = $storage->getChanges('replication');
            if (!count($changes)) {
                usleep($this->delay * 1_000_000);
                continue;
            }
            foreach ($changes as $change) {
                assert($change instanceof Change);
                if (!array_key_exists($change->tablename, $tableDrivers)) {
                    throw new Exception("Replication target not found");
                }
                foreach ($tableDrivers[$change->tablename] as $destination) {
                    switch ($change->action) {
                        case 'create':
                            $destination->findOrCreate($change->tablename, ['id' => $change->data['id']], $change->data);
                            break;
                        case 'update':
                            $destination->update($change->tablename, $change->data['id'], $change->data);
                            break;
                        case 'delete':
                            $destination->delete($change->tablename, $change->data['id']);
                            break;
                        default:
                            throw new \Exception('Unknown action');
                    }
                }
            }
            $storage->ackChanges($changes);
            $this->complete += count($changes);
        }
    }
}
