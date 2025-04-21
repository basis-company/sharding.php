<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Exception;

class Locator
{
    public function __construct(
        public readonly Database $database,
    ) {
    }

    public function castStorage(Bucket $bucket): void
    {
        if ($bucket->storage) {
            return;
        }

        $storages = $this->database->find(Storage::class);
        if (count($storages) !== 1) {
            throw new Exception('No storage casting');
        }

        [$storage] = $storages;

        $driver = $this->database->getStorageDriver($storage->id);
        $driver->update($this->database->meta->getClassTable(Bucket::class), $bucket->id, [
            'storage' => $storage->id,
        ]);

        $bucket->storage = $storage->id;
        $driver->syncSchema($this->database->meta->getSegmentByName($bucket->name), $this->database);
    }

    public function getBuckets(string $class, array $data = [], bool $create = false, bool $single = false): array
    {
        $bucketTable = $this->database->meta->getClassTable(Bucket::class);
        if (!$this->database->driver->hasTable($bucketTable)) {
            Bucket::initialize($this->database);
        }

        if ($class == Bucket::class) {
            $row = $this->database->driver->findOrFail($bucketTable, ['id' => Bucket::BUCKET_BUCKET_ID]);
            return [$this->database->createInstance(Bucket::class, $row)];
        }

        if (!class_exists($class)) {
            foreach (['.', '_'] as $candidate) {
                if (str_contains($class, $candidate)) {
                    $domain = explode($candidate, $class, 2)[0];
                    break;
                }
            }
        } else {
            $domain = $this->database->meta->getClassSegment($class)->prefix;
        }

        $buckets = $this->database->driver->find($bucketTable, ['name' => $domain]);
        $buckets = array_map(fn ($data) => $this->database->createInstance(Bucket::class, $data), $buckets);

        if ($single && count($buckets) > 1) {
            throw new Exception('Multiple buckets for ' . $class);
        }

        if (!count($buckets) && $create) {
            $buckets = [$this->database->create(Bucket::class, ['name' => $domain])];
        }
        if ($create) {
            array_walk($buckets, $this->castStorage(...));
        }

        return $buckets;
    }

}
