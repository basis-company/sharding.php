<?php

namespace Basis\Sharding\Job;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Tier;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Job;
use Exception;

class Configure implements Job
{
    public function __construct(
        public readonly string $class,
        public readonly ?int $shards = null,
        public readonly ?int $replicas = null,
        public readonly ?int $tier = null,
    ) {
    }

    public function replicas(int $replicas): self
    {
        return new self($this->class, $this->shards, $replicas, $this->tier);
    }

    public function shards(int $shards): self
    {
        return new self($this->class, $shards, $this->replicas, $this->tier);
    }

    public function tier(int $tier): self
    {
        return new self($this->class, $this->shards, $this->replicas, $tier);
    }

    public function __invoke(Database $database)
    {
        $name = $database->schema->getClassSegment($this->class)->fullname;

        $topologies = $database->find(Topology::class, ['name' => $name]);
        if (!count($topologies) && $database->schema->hasSegment($name)) {
            if (!$database->schema->getSegmentByName($name)->isSharded()) {
                throw new Exception("Invalid topology name: $name");
            }
            if ($this->tier === null && $tier = $database->schema->getClassModel($this->class)->getTier()) {
                $tier = $database->findOrCreate(Tier::class, ['name' => $tier])->id;
            }
            $topologies = [
                $database->findOrCreate(
                    Topology::class,
                    [
                        'name' => $name,
                        'version' => 1,
                    ],
                    [
                        'name' => $name,
                        'version' => 1,
                        'shards' => $this->shards ?: 1,
                        'replicas' => $this->replicas ?: 0,
                        'tier' => $this->tier ?: $tier ?: 1,
                        'status' => Topology::READY_STATUS,
                    ]
                ),
            ];
        }

        if (!count($topologies)) {
            throw new Exception("Invalid topology name: $name");
        }

        $last = array_pop($topologies);

        $updates = [];
        foreach (['shards', 'replicas', 'tier'] as $key) {
            if ($this->$key && $this->$key !== $last->$key) {
                $updates[$key] = $this->$key;
            }
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
