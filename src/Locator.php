<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Entity\Topology;
use Basis\Sharded\Interface\Locator as LocatorInterface;
use Basis\Sharded\Interface\Sharding as ShardingInterface;
use Basis\Sharded\Job\Configure;
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

        $usedStorageKeys = [];
        foreach ($database->find(Bucket::class) as $candidate) {
            if ($candidate->storage && $candidate->name === $bucket->name) {
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

    public function getBuckets(string $class, array $data = [], bool $writable = false, bool $single = false): array
    {
        if ($class == Bucket::class) {
            $row = $this->database->driver->findOrFail($this->bucketsTable, [
                'id' => Bucket::KEYS[Bucket::BUCKET_BUCKET_NAME]
            ]);
            return [$this->database->createInstance(Bucket::class, $row)];
        }

        if (class_exists($class)) {
            $bucketName = $this->database->schema->getClassSegment($class)->fullname;
        } else {
            $bucketName = $class;
            foreach (['.', '_'] as $candidate) {
                if (str_contains($class, $candidate)) {
                    $bucketName = explode($candidate, $class, 2)[0];
                    break;
                }
            }
        }

        $buckets = $this->database->driver->find($this->bucketsTable, ['name' => $bucketName]);
        $buckets = array_map(fn ($data) => $this->database->createInstance(Bucket::class, $data), $buckets);

        $topology = $this->getTopology($class);

        if (!count($buckets) && $writable) {
            foreach (range(0, $topology ? $topology->shards - 1 : 0) as $shard) {
                foreach (range(0, $topology ? $topology->replicas : 0) as $replica) {
                    $buckets[] = $this->database->create(Bucket::class, [
                        'name' => $bucketName,
                        'version' => $topology ? $topology->version : 0,
                        'shard' => $shard,
                        'replica' => $replica,
                    ]);
                }
            }
        }

        if (count($buckets) > 1 && $topology) {
            $key = (is_a($class, ShardingInterface::class, true) ? $class : self::class)::getKey($data);
            if ($key !== null) {
                $key = (((string) (int) $key) === $key) ? (int) $key : throw new Exception("Strings");
                $shard = $key % $topology->shards;
                $buckets = array_filter($buckets, fn (Bucket $bucket) => $bucket->shard == $shard);
            }
        }

        if ($single && count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        if ($writable) {
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
                $this->database->driver->update($this->bucketsTable, $bucket->id, ['storage' => $storage->id]);

                $driver = $this->database->getStorageDriver($storage->id);
                $driver->syncSchema($this->database, $bucket->name);
            });
        }

        return $buckets;
    }

    public static function getKey(array $data): ?string
    {
        return $data['id'] ?? null;
    }

    public function getTopology(string $class): ?Topology
    {
        if (!$this->database->schema->getClassModel($class)) {
            return null;
        }

        if (!$this->database->schema->getClassModel($class)->isSharded()) {
            return null;
        }

        $name = $this->database->schema->getClassSegment($class)->fullname;

        $topologies = $this->database->find(Topology::class, ['name' => $name]);

        if (!count($topologies)) {
            $topologies = [$this->database->dispatch(new Configure($name))];
        }

        return array_pop($topologies);
    }
}
