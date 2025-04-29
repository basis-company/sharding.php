<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Entity\Topology;
use Basis\Sharded\Interface\Locator as LocatorInterface;
use Basis\Sharded\Interface\Sharding as ShardingInterface;
use Basis\Sharded\Task\Configure;
use Exception;

class Locator implements LocatorInterface, ShardingInterface
{
    public string $bucketsTable;

    public function __construct(
        public readonly Database $database,
    ) {
        $this->bucketsTable = $database->schema->getClassTable(Bucket::class);
    }

    public static function castStorage(Database $database, Bucket $bucket): Storage
    {
        if ($bucket->storage) {
            return $database->findOrFail(Storage::class, ['id' => $bucket->storage]);
        }

        $storages = $database->find(Storage::class);
        if (count($storages) === 1) {
            return $storages[0];
        }

        $usedStorageKeys = [];
        foreach ($database->find(Bucket::class) as $candidate) {
            if ($candidate->name === $bucket->name) {
                $usedStorageKeys[] = $candidate->storage;
            }
        }

        $availableStorages = [];
        foreach ($storages as $storage) {
            if (!in_array($storage->id, $usedStorageKeys)) {
                $availableStorages[] = $storage;
            }
        }

        if (!count($availableStorages)) {
            throw new Exception('No available storage');
        }

        $usages = array_map(fn($storage) => $database->getStorageDriver($storage->id)->getUsage(), $availableStorages);
        return $availableStorages[array_search(min($usages), $usages)];
    }

    public function getBuckets(string $class, array $data = [], bool $create = false, bool $single = false): array
    {
        if ($class == Bucket::class) {
            $row = $this->database->driver->findOrFail($this->bucketsTable, [
                'id' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME]
            ]);
            return [$this->database->createInstance(Bucket::class, $row)];
        }

        if (!class_exists($class)) {
            foreach (['.', '_'] as $candidate) {
                if (str_contains($class, $candidate)) {
                    $bucketName = explode($candidate, $class, 2)[0];
                    break;
                }
            }
        } else {
            $bucketName = $this->database->schema->getClassSegment($class)->fullname;
        }

        $buckets = $this->database->driver->find($this->bucketsTable, ['name' => $bucketName]);
        $buckets = array_map(fn ($data) => $this->database->createInstance(Bucket::class, $data), $buckets);

        if (!count($buckets) && $create) {
            $topology = new Topology(0, '', 0, 1, 0);
            if ($this->database->schema->getClassModel($class)->isSharded()) {
                $topologies = $this->database->find(Topology::class, [
                    'name' => $bucketName
                ]);
                if (!count($topologies)) {
                    $topologies = [$this->database->dispatch(new Configure($bucketName))];
                }
                $topology = array_pop($topologies);
            }

            foreach (range(0, $topology->shards - 1) as $shard) {
                foreach (range(0, $topology->replicas) as $replica) {
                    $buckets[] = $this->database->create(Bucket::class, [
                        'name' => $bucketName,
                        'version' => $topology->version,
                        'shard' => $shard,
                        'replica' => $replica,
                    ]);
                }
            }
        }

        if ($single && count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        if ($create) {
            array_walk($buckets, function (Bucket $bucket) use ($class) {
                if ($bucket->storage) {
                    return;
                }
                if (is_a($class, LocatorInterface::class, true)) {
                    $storage = $class::castStorage($this->database, $bucket);
                } else {
                    $storage = $this->castStorage($this->database, $bucket);
                }

                $bucket->storage = $storage->id;

                $driver = $this->database->getStorageDriver($storage->id);
                $driver->update($this->bucketsTable, $bucket->id, ['storage' => $storage->id]);

                $segment = $this->database->schema->getSegmentByName($bucket->name);
                $driver->syncSchema($segment, $this->database);
            });
        }

        return $buckets;
    }

    public static function getKey(array $data): ?string
    {
        return $data['id'] ?? null;
    }
}
