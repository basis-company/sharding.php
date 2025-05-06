<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Job;
use Exception;

class Configure implements Job
{
    public function __construct(
        public readonly string $name,
        public readonly ?int $shards = null,
        public readonly ?int $replicas = null,
    ) {
    }

    public function shards(int $shards): self
    {
        return new self($this->name, $shards, $this->replicas);
    }

    public function replicas(int $replicas): self
    {
        return new self($this->name, $this->shards, $replicas);
    }

    public function __invoke(Database $database)
    {
        $topologies = $database->find(Topology::class, ['name' => $this->name]);
        if (!count($topologies) && $database->schema->hasSegment($this->name)) {
            if (!$database->schema->getSegmentByName($this->name)->isSharded()) {
                throw new Exception("Invalid topology name: $this->name");
            }
            $topologies = [
                $database->findOrCreate(
                    Topology::class,
                    [
                        'name' => $this->name,
                        'version' => 1,
                    ],
                    [
                        'name' => $this->name,
                        'version' => 1,
                        'shards' => $this->shards ?: 1,
                        'replicas' => $this->replicas ?: 0,
                        'status' => Topology::READY_STATUS,
                    ]
                ),
            ];
        }

        if (!count($topologies)) {
            throw new Exception("Invalid topology name: $this->name");
        }

        $last = array_pop($topologies);

        $updates = [];

        if ($this->shards && $this->shards !== $last->shards) {
            $updates['shards'] = $this->shards;
        }

        if ($this->replicas && $this->replicas !== $last->replicas) {
            $updates['replicas'] = $this->replicas;
        }

        if (count($updates)) {
            if ($last->status !== Topology::READY_STATUS) {
                throw new Exception("Topology is not ready");
            }

            $buckets = $database->find(Bucket::class, [
                'name' => $last->name,
                'version' => $last->version,
            ]);

            $last = clone($last);
            $last->id = 0;
            $last->version++;
            $last->status = count($buckets) ? Topology::DRAFT_STATUS : Topology::READY_STATUS;

            foreach ($updates as $key => $value) {
                $last->$key = $value;
            }

            return $database->findOrCreate(
                Topology::class,
                [
                    'name' => $last->name,
                    'version' => $last->version,
                ],
                get_object_vars($last),
            );
        }

        return $last;
    }
}
