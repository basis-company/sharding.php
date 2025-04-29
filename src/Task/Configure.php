<?php

namespace Basis\Sharded\Task;

use Basis\Sharded\Entity\Topology;
use Basis\Sharded\Interface\Database;
use Basis\Sharded\Interface\Task;
use Exception;

class Configure implements Task
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
                        'shards' => 1,
                        'replicas' => 0,
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
            $last = clone($last);
            $last->id = 0;
            $last->version++;

            foreach ($updates as $key => $value) {
                $last->$key = $value;
            }

            return $database->findOrCreate(
                Topology::class,
                [
                    'name' => $last->name,
                    'version' => $last->version
                ],
                get_object_vars($last),
            );
        }

        return $last;
    }
}
