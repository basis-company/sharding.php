<?php

namespace Basis\Sharded\Driver;

use Basis\Sharded\Database;
use Basis\Sharded\Interface\Bootstrap;
use Basis\Sharded\Interface\Driver;
use Basis\Sharded\Schema\Segment;
use Exception;
use Tarantool\Client\Client;
use Tarantool\Client\Schema\Operations;
use Tarantool\Mapper\Mapper;

class Tarantool implements Driver
{
    public readonly Mapper $mapper;

    public function __construct(
        protected readonly string $dsn,
    ) {
        $this->mapper = new Mapper(Client::fromDsn($dsn));
    }

    public function create(string $table, array $data): object
    {
        return $this->mapper->create($table, $data);
    }

    public function delete(string|object $table, ?int $id = null): ?object
    {
        return $this->mapper->delete($table, ['id' => $id]);
    }

    public function find(string $table, array $query = []): array
    {
        return $this->mapper->find($table, $query);
    }

    public function findOne(string $table, array $query): ?object
    {
        return $this->mapper->findOne($table, $query);
    }

    public function findOrCreate(string $table, array $query, array $data = []): object
    {
        return $this->mapper->findOrCreate($table, $query, $data);
    }

    public function findOrFail(string $table, array $query): ?object
    {
        $row = $this->findOne($table, $query);
        if (!$row) {
            throw new Exception('No ' . $table . ' found');
        }
        return $row;
    }

    public function getDsn(): string
    {
        return $this->dsn;
    }

    public function getMapper(): Mapper
    {
        return $this->mapper;
    }

    public function getTarantoolType(string $type): string
    {
        return match ($type) {
            'int' => 'unsigned',
            'string' => 'string',
            default => throw new Exception('Invalid type'),
        };
    }

    public function hasTable(string $table): bool
    {
        return $this->mapper->hasSpace($table);
    }

    public function syncSchema(Segment $segment, Database $database): void
    {
        $bootstrap = [];
        foreach ($segment->getModels() as $model) {
            $present = $this->mapper->hasSpace($model->table);
            if ($present) {
                $space = $this->mapper->getSpace($model->table);
            } else {
                $space = $this->mapper->createSpace($model->table, [
                    'if_not_exists' => true,
                ]);
            }
            if (!count($space->getFields())) {
                $space->addProperty('bucket', 'unsigned');
            }

            foreach ($model->getProperties() as $property) {
                if (in_array($property->name, $space->getFields())) {
                    continue;
                }
                $space->addProperty($property->name, $this->getTarantoolType($property->type));
            }
            foreach ($model->getIndexes() as $index) {
                $space->addIndex($index->fields, [
                    'if_not_exists' => true,
                    'name' => $index->getName(),
                    'unique' => $index->unique,
                ]);
            }
            $space->addIndex(['bucket'], [
                'if_not_exists' => true,
                'name' => 'bucket',
                'unique' => false,
            ]);

            if (!$present && is_a($model->class, Bootstrap::class, true)) {
                $bootstrap[] = $model->class;
            }
        }

        foreach ($bootstrap as $class) {
            $class::bootstrap($database);
        }
    }
    public function reset(): self
    {
        $this->mapper->dropUserSpaces();

        return $this;
    }

    public function update(string|object $table, int|array $id, ?array $data = null): ?object
    {
        $operations = null;
        foreach ($data as $k => $v) {
            if ($operations instanceof Operations) {
                $operations = $operations->andSet($k, $v);
            } else {
                $operations = Operations::set($k, $v);
            }
        }

        $changes = $this->mapper->client->getSpace($table)->update([$id], $operations);
        return count($changes) ? (object) $changes[0] : null;
    }
}
