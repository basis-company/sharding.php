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
    public ?array $stats = null;

    public static function getKey(array $data): int|string|null
    {
        return $data['id'] ?? null;
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

    public function __construct(
        public readonly Database $database,
    ) {
    }

    public function assignStorage(Bucket $bucket, $class)
    {
        if (!$bucket->storage) {
            $casting = [is_a($class, LocatorInterface::class, true) ? $class : self::class, 'castStorage'];
            $this->database->update($bucket, ['storage' => call_user_func($casting, $this->database, $bucket)->id]);
        }

        if ($this->database->schema->hasSegment($bucket->name)) {
            $this->database->getStorageDriver($bucket->storage)->syncSchema($this->database, $bucket);
        }

        if ($bucket->version && !$bucket->replica) {
            $topology = $this->database->findOrFail(Topology::class, [
                'name' => $bucket->name,
                'version' => $bucket->version,
            ]);
            if ($topology->replicas) {
                $driver = $this->database->getStorageDriver($bucket->storage);
                array_map(
                    fn($table) => $driver->registerChanges($table, 'replication'),
                    $this->database->schema->getSegmentByName($bucket->name)->getTables(),
                );
            }
        }
    }

    public function generateBuckets(Topology $topology): array
    {
        $buckets = [];

        foreach (range(1, $topology->shards) as $shard) {
            foreach (range(0, $topology->replicas) as $replica) {
                $buckets[] = $this->database->findOrCreate(Bucket::class, [
                    'name' => $topology->name,
                    'version' => $topology->version,
                    'shard' => $shard - 1,
                    'replica' => $replica,
                ]);
            }
        }

        return $buckets;
    }

    public function getBuckets(string $class, array $data = [], bool $writable = false, bool $multiple = true): array
    {
        $driver = $this->database->getStorageDriver(1);

        if ($class == Bucket::class) {
            $row = $driver->findOrFail(Bucket::TABLE, ['name' => Bucket::BUCKET]);
            return [$this->database->factory->getInstance(Bucket::class, $row)];
        }

        if (class_exists($class)) {
            $name = $this->database->schema->getClassSegment($class)->fullname;
        } else {
            $name = $class;
            foreach (['.', '_'] as $candidate) {
                if (str_contains($class, $candidate)) {
                    $name = explode($candidate, $class, 2)[0];
                    break;
                }
            }
        }

        $buckets = $driver->find(Bucket::TABLE, ['name' => $name]);
        $buckets = array_map(fn ($data) => $this->database->factory->getInstance(Bucket::class, $data), $buckets);

        $topology = $this->getTopology($class);
        if ($topology) {
            $buckets = array_filter($buckets, fn ($bucket) => $bucket->version == $topology->version);
        }

        if (!count($buckets)) {
            $buckets = $this->generateBuckets($topology ?: new Topology(0, $name, 0, Topology::READY_STATUS, 1, 0));
        }

        $buckets = array_filter($buckets, fn (Bucket $bucket) => $bucket->replica == !$writable) ?: $buckets;

        if ($topology && count($buckets) > 1) {
            $shard = $this->getShard($topology, $class, $data);
            if ($shard !== null) {
                $buckets = array_filter($buckets, fn (Bucket $bucket) => $bucket->shard == $shard);
            }
        }

        if (!$multiple && count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        array_walk($buckets, fn($bucket) => $this->assignStorage($bucket, $class, $topology));

        if ($topology && $topology->replicas > 1) {
            shuffle($buckets);
            $groups = [];
            foreach ($buckets as $bucket) {
                $groups[$bucket->shard] = $bucket;
            }
            $buckets = array_values($groups);
        }

        if ($this->stats !== null) {
            foreach ($buckets as $bucket) {
                if (!array_key_exists($bucket->id, $this->stats)) {
                    $this->stats[$bucket->id] = 1;
                } else {
                    $this->stats[$bucket->id]++;
                }
            }
        }

        return array_values($buckets);
    }

    public function getShard(Topology $topology, string $class, array $data): ?int
    {
        $key = (is_a($class, ShardingInterface::class, true) ? $class : self::class)::getKey($data);

        if ($key !== null) {
            if (((string) (int) $key) === $key) {
                $key = (int) $key;
            }
            if (!is_int($key)) {
                $key = abs(crc32(strval($key)));
            }
            return $key % $topology->shards;
        }

        return null;
    }

    public function getTopology(string $class, string $status = Topology::READY_STATUS): ?Topology
    {
        if (!$this->database->schema->getClassModel($class)) {
            return null;
        }

        if (!$this->database->schema->getClassModel($class)->isSharded()) {
            return null;
        }

        $name = $this->database->schema->getClassSegment($class)->fullname;

        $topologies = $this->database->find(Topology::class, [
            'name' => $name,
        ]);

        $topologies = array_filter($topologies, fn(Topology $topology) => $topology->status == $status);

        if (!count($topologies)) {
            $topologies = [$this->database->dispatch(new Configure($class))];
        }

        return array_pop($topologies);
    }
}
