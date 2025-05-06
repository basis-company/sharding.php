<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Interface\Locator as LocatorInterface;
use Basis\Sharding\Interface\Sharding as ShardingInterface;
use Basis\Sharding\Job\Configure;
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

        $buckets = array_filter(
            $database->find(Bucket::class, ['name' => $bucket->name]),
            fn($bucket) => $bucket->storage > 0
        );

        $usedStorageKeys = array_map(fn($bucket) => $bucket->storage, $buckets);
        $storages = $database->find(Storage::class);
        $availableStorages = array_filter($storages, fn($storage) => !in_array($storage->id, $usedStorageKeys));

        if (!count($availableStorages)) {
            throw new Exception('No available storage');
        }

        $usages = array_map(fn($storage) => $database->getStorageDriver($storage->id)->getUsage(), $availableStorages);
        return $availableStorages[array_search(min($usages), $usages)];
    }

    public function getBuckets(string $class, array $data = [], bool $writable = false): array
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
        if ($topology) {
            $buckets = array_filter($buckets, fn ($bucket) => $bucket->version == $topology->version);
        }

        if (!count($buckets)) {
            foreach (range(0, $topology ? $topology->shards - 1 : 0) as $shard) {
                foreach (range(0, $topology ? $topology->replicas : 0) as $replica) {
                    $bucket = $this->database->create(Bucket::class, [
                        'name' => $bucketName,
                        'version' => $topology ? $topology->version : 0,
                        'shard' => $shard,
                        'replica' => $replica,
                    ]);
                    if (!$writable || ($writable && !$replica)) {
                        $buckets[] = $bucket;
                    }
                }
            }
        }

        if ($topology && count($buckets) > 1) {
            $key = (is_a($class, ShardingInterface::class, true) ? $class : self::class)::getKey($data);
            if ($key !== null) {
                if (((string) (int) $key) === $key) {
                    $key = (int) $key;
                }
                if (!is_int($key)) {
                    $key = abs(crc32(strval($key)));
                }
                $shard = $key % $topology->shards;
                $buckets = array_filter($buckets, fn (Bucket $bucket) => $bucket->shard == $shard);
            }
        }

        if ($writable && count($buckets) > 1) {
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

    public static function getKey(array $data): int|string|null
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
